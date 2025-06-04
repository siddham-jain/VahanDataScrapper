from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.action_chains import ActionChains
from selenium.common.exceptions import TimeoutException, ElementNotInteractableException, StaleElementReferenceException, WebDriverException, NoSuchElementException
import time
import os
import re
import random
import datetime
import logging
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service
import boto3
# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class RTOWiseProcessor:
    def __init__(self, base_download_dir):
        self.base_download_dir = base_download_dir
        self.setup_driver()
        self.setup_directories()
        self.URL = "https://vahan.parivahan.gov.in/vahan4dashboard/vahan/view/reportview.xhtml"
        self.s3_bucket_name = "vahan-rto-data"
        self.s3_base_prefix = "rto-2024/"  # optional S3 prefix path
        self.s3_client = boto3.client('s3')
        
        
    def setup_driver(self):
        """Initialize Chrome WebDriver with proper configurations"""
        options = webdriver.ChromeOptions()
        options.add_argument("--headless=new")  # Use headless mode
        options.add_argument("--no-sandbox")    # Required for EC2
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-notifications")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option("useAutomationExtension", False)
        
        user_agents = [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/99.0.4844.51 Safari/537.36",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/100.0.4896.75 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/15.4 Safari/605.1.15"
        ]
        options.add_argument(f"--user-agent={random.choice(user_agents)}")

        os.makedirs(self.base_download_dir, exist_ok=True)
        
        prefs = {
            "download.default_directory": self.base_download_dir,
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True
        }
        options.add_experimental_option("prefs", prefs)
        
        self.driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)
        
        # Anti-detection script
        self.driver.execute_cdp_cmd("Page.addScriptToEvaluateOnNewDocument", {
            "source": """
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            """
        })

    def setup_directories(self):
        """Create necessary directory structure"""
        self.base_download_dir = os.path.join(os.getcwd(), "rto_2024")
        os.makedirs(self.base_download_dir, exist_ok=True)
    
    def log_message(self, message):
        """Log messages with timestamp"""
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        log_entry = f"[{timestamp}] {message}"
        print(log_entry)
    
    def random_delay(self, min_seconds=0.3, max_seconds=1.0):
        """Add random delay to mimic human behavior"""
        delay = random.uniform(min_seconds, max_seconds)
        time.sleep(delay)
        return delay
    
    def smart_click(self, element, element_name="element"):
        """Try multiple click methods until one works"""
        methods = [
            lambda: element.click(),
            lambda: self.driver.execute_script("arguments[0].click();", element),
            lambda: ActionChains(self.driver).move_to_element(element).click().perform(),
        ]
        
        for i, method in enumerate(methods):
            try:
                method()
                self.log_message(f"Clicked {element_name} using method {i+1}")
                return True
            except Exception as e:
                if i == len(methods) - 1:
                    self.log_message(f"All click methods failed for {element_name}: {str(e)}")
                    return False
                continue
        return False
    
    def wait_and_find_element(self, locator_type, locator_value, timeout=10, name="element"):
        """Wait for element and scroll to it"""
        try:
            element = WebDriverWait(self.driver, timeout).until(
                EC.presence_of_element_located((locator_type, locator_value))
            )
            self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            self.random_delay(0.2, 0.5)
            return element
        except Exception as e:
            self.log_message(f"Failed to find {name}: {str(e)}")
            return None
    
    def get_all_rtos_for_state(self):
        """Get list of all RTOs for the currently selected state"""
        try:
            self.log_message("Getting list of all RTOs for the selected state")
            
            # Click RTO dropdown to open it
            rto_dropdown_label = self.wait_and_find_element(By.ID, "selectedRto_label", 20, "RTO dropdown label")
            if not rto_dropdown_label:
                return []
            
            self.smart_click(rto_dropdown_label, "RTO dropdown label")
            self.random_delay(0.5, 1)
            
            # Find all RTO options
            rto_options = self.driver.find_elements(By.XPATH, "//div[@id='selectedRto_panel']//li")
            
            rto_list = []
            for option in rto_options:
                rto_text = option.text.strip()
                if rto_text and "All Vahan4 Running Office" not in rto_text:
                    rto_list.append(rto_text)
            
            # Close dropdown by clicking outside
            self.driver.find_element(By.TAG_NAME, "body").click()
            
            self.log_message(f"Found {len(rto_list)} RTOs for the state")
            return rto_list
        except Exception as e:
            self.log_message(f"Error in get_all_rtos_for_state: {str(e)}")
            return []
    
    def select_specific_rto(self, rto_name):
        """Select a specific RTO from dropdown"""
        try:
            self.log_message(f"Selecting RTO: {rto_name}")
            
            # Open RTO dropdown
            rto_dropdown_label = self.wait_and_find_element(By.ID, "selectedRto_label", 10, "RTO dropdown")
            if not rto_dropdown_label:
                return False
            
            self.smart_click(rto_dropdown_label, "RTO dropdown")
            self.random_delay(0.5, 1)
            
            # Find and click specific RTO
            rto_xpath = f"//li[normalize-space(text())='{rto_name}']"
            rto_option = self.wait_and_find_element(By.XPATH, rto_xpath, 5, f"RTO option: {rto_name}")
            
            if rto_option:
                self.smart_click(rto_option, f"RTO option: {rto_name}")
                self.log_message(f"Successfully selected RTO: {rto_name}")
                return True
            else:
                self.log_message(f"Could not find RTO: {rto_name}")
                return False
                
        except Exception as e:
            self.log_message(f"Error selecting RTO {rto_name}: {str(e)}")
            return False
    
    def setup_page_for_rto_processing(self):
        """Setup page with proper axis configuration for RTO processing"""
        try:
            # Load page
            self.driver.get(self.URL)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[name='javax.faces.ViewState']"))
            )
            self.random_delay(1, 2)
            
            # Setup Y-axis (Maker)
            y_axis_label = self.wait_and_scroll_to_element(By.ID, "yaxisVar_label", 20, "Y-axis dropdown")
            if y_axis_label:
                self.smart_click(y_axis_label, "Y-axis dropdown")
                self.random_delay(0.5, 1)
                
                maker_option = self.wait_and_find_element(By.XPATH, "//li[@data-label='Maker']", 10, "Maker option")
                if maker_option:
                    self.smart_click(maker_option, "Maker option")
            
            # Setup X-axis (Month Wise)
            x_axis_label = self.wait_and_scroll_to_element(By.ID, "xaxisVar_label", 20, "X-axis dropdown")
            if x_axis_label:
                self.smart_click(x_axis_label, "X-axis dropdown")
                self.random_delay(0.5, 1)
                
                month_option = self.wait_and_find_element(By.XPATH, "//li[@data-label='Month Wise']", 10, "Month Wise option")
                if month_option:
                    self.smart_click(month_option, "Month Wise option")
            
            self.log_message("Page setup completed")
            return True
        except Exception as e:
            self.log_message(f"Error in page setup: {str(e)}")
            return False
        
    def wait_and_scroll_to_element(self, locator_type, locator_value, timeout=10, name="element"):
        try:
            # First try with regular wait
            try:
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((locator_type, locator_value))
                )
            except StaleElementReferenceException:
                self.log_message(f"Stale element when waiting for {name}, retrying...")
                # Handle stale reference by refreshing the wait
                element = WebDriverWait(self.driver, timeout).until(
                    EC.presence_of_element_located((locator_type, locator_value))
                )
            except TimeoutException:
                self.log_message(f"Timeout waiting for {name}, checking if page needs refresh...")
                # Check if we need to refresh the page due to session timeout
                try:
                    error_message = self.driver.find_element(By.XPATH, "//span[contains(text(), 'session')]")
                    if error_message and "session" in error_message.text.lower():
                        self.log_message("Session timeout detected, refreshing page...")
                        self.driver.refresh()
                        WebDriverWait(self.driver, 30).until(
                            EC.presence_of_element_located((By.CSS_SELECTOR, "[name='javax.faces.ViewState']"))
                        )
                        # Try again after refresh
                        element = WebDriverWait(self.driver, timeout).until(
                            EC.presence_of_element_located((locator_type, locator_value))
                        )
                    else:
                        raise
                except:
                    self.log_message(f"Element {name} not found after timeout")
                    return None
            
            # Once we have the element, scroll to it
            try:
                self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
                self.random_delay(0.2, 0.5)
            except Exception as e:
                self.log_message(f"Warning: Could not scroll to {name}: {str(e)}")
            
            return element
        except WebDriverException as e:
            if "not reachable" in str(e) or "connection" in str(e).lower() or "timeout" in str(e).lower():
                self.log_message(f"Connection error or timeout when finding {name}: {str(e)}")
                # Check if it's a timeout issue from the website
                if "timeout" in str(e).lower():
                    self.log_message("Website timeout detected. Waiting for 15 minutes before retrying...")
                    time.sleep(900)  # Wait for 15 minutes (900 seconds)
                    self.log_message("Resuming after 15-minute wait")
                
                # Try to recover by refreshing
                try:
                    self.driver.refresh()
                    self.log_message("Page refreshed after connection error or timeout")
                    return self.wait_and_scroll_to_element(locator_type, locator_value, timeout, name)
                except:
                    self.log_message("Failed to recover from connection error or timeout")
                    return None
            else:
                self.log_message(f"WebDriver error when finding {name}: {str(e)}")
                return None
        except Exception as e:
            self.log_message(f"Failed to find {name} ({locator_type}:{locator_value}): {str(e)}")
            return None

    def setup_axis(self):
        try:
            self.log_message("Setting up X-axis (Month Wise) and Y-axis (Maker)...")

            self.driver.get(self.URL)
            WebDriverWait(self.driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[name='javax.faces.ViewState']"))
            )
            # self.random_delay(1, 2)
            
            # Select Y-axis (Maker)
            y_axis_label = self.wait_and_scroll_to_element(By.ID, "yaxisVar_label", 20, "Y-axis dropdown")
            if not y_axis_label:
                self.log_message("Could not find Y-axis dropdown")
                return False
                
            self.smart_click(y_axis_label, "Y-axis dropdown")
            self.random_delay(0.5, 1)
            
            maker_option = self.wait_and_scroll_to_element(By.XPATH, "//li[@data-label='Maker']", 10, "Maker option")
            if maker_option:
                self.smart_click(maker_option, "Maker option")
            else:
                # Try JavaScript fallback
                self.driver.execute_script("PrimeFaces.widgets.widget_yaxisVar.selectValue('4');")
                self.log_message("Used JavaScript to select Maker for Y-axis")
            self.random_delay(0.5, 1)
            
            # Select X-axis (Month Wise)
            x_axis_label = self.wait_and_scroll_to_element(By.ID, "xaxisVar_label", 20, "X-axis dropdown")
            if not x_axis_label:
                self.log_message("Could not find X-axis dropdown")
                return False
                
            self.smart_click(x_axis_label, "X-axis dropdown")
            self.random_delay(0.5, 1)
            
            month_wise_option = self.wait_and_scroll_to_element(By.XPATH, "//li[@data-label='Month Wise']", 10, "Month Wise option")
            if month_wise_option:
                self.smart_click(month_wise_option, "Month Wise option")
            else:
                # Try JavaScript fallback
                self.driver.execute_script("PrimeFaces.widgets.widget_xaxisVar.selectValue('6');")
                self.log_message("Used JavaScript to select Month Wise for X-axis")
            self.random_delay(0.5, 1)
            
            self.log_message("Axis setup completed")
            return True
        except Exception as e:
            self.log_message(f"Error in setup_axis: {str(e)}")
            return False

    def select_state(self, state_name):
        """Select state from dropdown"""
        try:
            self.log_message(f"Selecting State: {state_name}")
            
            state_dropdown = self.wait_and_find_element(By.ID, "j_idt49_label", 20, "state dropdown")
            if not state_dropdown:
                return False
            
            self.smart_click(state_dropdown, "state dropdown")
            self.random_delay(0.5, 1)
            
            state_base_name = state_name.split('(')[0].strip()
            state_option = self.wait_and_find_element(By.XPATH, f"//li[contains(text(), '{state_base_name}')]", 10, f"state option: {state_name}")
            
            if state_option:
                self.smart_click(state_option, f"state option: {state_name}")
                self.log_message(f"Successfully selected state: {state_name}")
                return True
            return False
        except Exception as e:
            self.log_message(f"Error selecting state: {str(e)}")
            return False

    def select_state_primefaces(self, state_name):
        try:
            self.log_message(f"Selecting State: {state_name}")
        
            # First find and click the state dropdown label to open the dropdown
            state_dropdown_label = self.wait_and_scroll_to_element(By.ID, "j_idt49_label", 20, "state dropdown label")
            if not state_dropdown_label:
                # Try alternative locators
                state_dropdown_label = self.wait_and_scroll_to_element(By.XPATH, "//div[contains(@id, 'j_idt49_label')]//label", 5, "state dropdown alternative")
                if not state_dropdown_label:
                    self.log_message("Could not find state dropdown label")
                    return False
            
            self.smart_click(state_dropdown_label, "state dropdown label")
            self.random_delay(0.5, 1)
            
            # Extract the base name of the state (without numbers)
            state_base_name = state_name.split('(')[0].strip()
            
            # Try multiple approaches to find and click the state item
            methods = [
                # Method 1: Exact text match
                lambda: self.driver.find_element(By.XPATH, f"//li[normalize-space(text())='{state_name}']"),
                # Method 2: Contains text
                lambda: self.driver.find_element(By.XPATH, f"//li[contains(text(), '{state_base_name}')]"),
                # Method 3: Using data-label attribute
                lambda: self.driver.find_element(By.XPATH, f"//li[@data-label='{state_name}']"),
                # Method 4: Partial data-label match
                lambda: self.driver.find_element(By.XPATH, f"//li[contains(@data-label, '{state_base_name}')]"),
                # Method 5: Using ID containing 'j_idt39' and containing text
                lambda: self.driver.find_element(By.XPATH, f"//ul[contains(@id, 'j_idt49')]/li[contains(text(), '{state_base_name}')]")
            ]
            
            for method in methods:
                try:
                    state_option = method()
                    self.smart_click(state_option, f"state option: {state_name}")
                    self.random_delay(0.5, 1)
                    
                    # Verify selection was successful
                    current_selection = self.driver.find_element(By.ID, "j_idt49_label").text
                    if state_base_name in current_selection:
                        self.log_message(f"Successfully selected state: {current_selection}")
                        return True
                    else:
                        self.log_message(f"Selection verification failed. Current selection: {current_selection}")
                        continue
                except Exception:
                    continue
            
            # If all methods fail, try using JavaScript with PrimeFaces
            try:
                for i, state in enumerate(self.states_to_process):
                    if state_name in state:
                        js_command = f"PrimeFaces.widgets.widget_j_idt49.selectValue('{i}');"
                        self.driver.execute_script(js_command)
                        self.log_message(f"Attempted to select state using JavaScript: {js_command}")
                        self.random_delay(0.5, 1)
                        
                        # Verify selection
                        current_selection = self.driver.find_element(By.ID, "j_idt49_label").text
                        if state_base_name in current_selection:
                            self.log_message(f"JavaScript selection successful: {current_selection}")
                            return True
                
                self.log_message("Failed to select state via JavaScript")
                return False
            except Exception as e:
                self.log_message(f"JavaScript selection failed: {str(e)}")
                return False
                
        except Exception as e:
            self.log_message(f"Error in select_state_primefaces: {str(e)}")
            return False
    
    def select_year(self, year):
        """Select year from dropdown"""
        try:
            self.log_message(f"Selecting Year: {year}")
            
            year_dropdown = self.wait_and_find_element(By.ID, "selectedYear_label", 20, "year dropdown")
            if not year_dropdown:
                return False
            
            self.smart_click(year_dropdown, "year dropdown")
            self.random_delay(0.5, 1)
            
            year_option = self.wait_and_find_element(By.XPATH, f"//li[text()='{year}']", 10, f"year option: {year}")
            if year_option:
                self.smart_click(year_option, f"year option: {year}")
                self.log_message(f"Successfully selected year: {year}")
                return True
            return False
        except Exception as e:
            self.log_message(f"Error selecting year: {str(e)}")
            return False
    
    def open_left_panel(self):
        try:
            self.log_message("Opening left panel if needed")
            
            # Check if panel needs to be expanded by looking at the toggler
            toggler = self.driver.find_element(By.ID, "filterLayout-toggler")
            panel_class = toggler.get_attribute("class")
            
            if "ui-layout-toggler-closed" in panel_class or "layout-toggler-collapsed" in panel_class:
                self.smart_click(toggler, "left panel toggler")
                self.log_message("Expanded left panel")
                self.random_delay(0.5, 1)
            else:
                self.log_message("Left panel already open")
            
            return True
        except Exception as e:
            self.log_message(f"Error in open_left_panel: {str(e)}")
            # Not critical if this fails - we can still try to select options
            return True
    
    def close_left_panel_if_opened(self):
        try:
            # Find the collapse button based on its attributes
            collapse_button = self.driver.find_element(
                By.XPATH, '//a[@title="Collapse" and contains(@class, "ui-layout-unit-header-icon")]'
            )
            if collapse_button.is_displayed():
                collapse_button.click()
                self.random_delay(0.5, 1.5)  # Small delay to allow UI to collapse
                self.log_message("Left panel collapsed successfully.")
            else:
                self.log_message("Collapse button not visible; panel might already be closed.")
        except NoSuchElementException:
            self.log_message("Collapse button not found; left panel may already be closed or selector is incorrect.")
        except Exception as e:
            self.log_message(f"Unexpected error while closing left panel: {str(e)}")
    
    def select_left_panel_option(self):
        try:
            self.log_message("Selecting vehicle categories and fuel types")
            
            # Select Vehicle Categories: TWO WHEELER
            for idx in [0, 1, 2]:  # Indices for TWO WHEELER categories
                try:
                    checkbox_id = f"VhCatg:{idx}"
                    checkbox = self.wait_and_scroll_to_element(By.ID, checkbox_id, 5, f"checkbox {checkbox_id}")
                    if checkbox and not checkbox.is_selected():
                        self.smart_click(checkbox, f"checkbox {checkbox_id}")
                        self.random_delay(0.2, 0.5)
                except Exception as e:
                    self.log_message(f"Error selecting checkbox VhCatg:{idx}: {str(e)}")
            
            # Select Fuel Types: EV Only
            for idx in [7, 21]:
                try:
                    checkbox_id = f"fuel:{idx}"
                    checkbox = self.wait_and_scroll_to_element(By.ID, checkbox_id, 5, f"checkbox {checkbox_id}")
                    if checkbox and not checkbox.is_selected():
                        self.smart_click(checkbox, f"checkbox {checkbox_id}")
                        self.random_delay(0.2, 0.5)
                except Exception as e:
                    self.log_message(f"Error selecting checkbox fuel:{idx}: {str(e)}")
            
            self.log_message("Vehicle categories and fuel types selected")
            return True
        except Exception as e:
            self.log_message(f"Error in select_left_panel_options: {str(e)}")
            return False

    def upload_to_s3(self, local_path, s3_key):
        """
        Upload a file to S3 bucket
        
        Args:
            local_path (str): Path to the local file to upload
            s3_key (str): S3 object key (path in the bucket)
            
        Returns:
            bool: True if upload was successful, False otherwise
        """
        try:
            start_time = time.time()
            file_size = os.path.getsize(local_path) / (1024 * 1024)  # Size in MB
            
            self.log_message(f"Uploading {local_path} to s3://{self.s3_bucket_name}/{s3_key} "
                           f"(Size: {file_size:.2f} MB)")
            
            # Upload the file with public-read ACL (adjust as needed)
            self.s3_client.upload_file(
                local_path,
                self.s3_bucket_name,
                s3_key,
                ExtraArgs={
                    'ACL': 'bucket-owner-full-control',  # Adjust based on your requirements
                    'ContentType': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                }
            )
            
            upload_time = time.time() - start_time
            self.log_message(f"Upload completed in {upload_time:.2f} seconds "
                           f"(Speed: {file_size/upload_time:.2f} MB/s)")
            
            # Verify the file exists in S3
            try:
                self.s3_client.head_object(Bucket=self.s3_bucket_name, Key=s3_key)
                return True
            except Exception as e:
                self.log_message(f"Verification failed - file not found in S3: {str(e)}")
                return False
                
        except FileNotFoundError:
            self.log_message(f"Local file not found: {local_path}")
            return False
        except Exception as e:
            self.log_message(f"Error uploading to S3: {str(e)}")
            return False

    def click_left_refresh(self):
        try:
            self.log_message("Clicking left refresh button")
            
            refresh_button = self.wait_and_scroll_to_element(By.ID, "j_idt89", 20, "left refresh button")
            if not refresh_button:
                self.log_message("Could not find left refresh button")
                return False
                
            self.smart_click(refresh_button, "left refresh button")
            self.random_delay(2, 4)  # Longer delay for processing
            self.log_message("Left refresh button clicked")
            return True
        except Exception as e:
            self.log_message(f"Error in click_left_refresh: {str(e)}")
            return False

    def apply_ev_filters(self):
        """Apply EV-specific filters"""
        try:
            self.log_message("Applying EV filters")
            
            # Click right refresh to load data
            refresh_button = self.wait_and_find_element(By.ID, "j_idt83", 20, "right refresh button")
            if refresh_button:
                self.smart_click(refresh_button, "right refresh button")
                self.random_delay(4, 5)

            # Open LEFT PANEL options
            self.open_left_panel()

            self.random_delay(0.5, 1)
            
            # Select TWO WHEELER categories
            self.select_left_panel_option()

            self.random_delay(0.5, 1)

            # Click LEFT REFRESH
            self.click_left_refresh()

            self.random_delay(3.5, 4)

            # Close LEFT PANEL if opened
            self.close_left_panel_if_opened()
            
            return True
        except Exception as e:
            self.log_message(f"Error applying filters: {str(e)}")
            return False
    
    def download_excel_rto(self, state_name, year, rto_name):
        try:
            self.log_message(f"Downloading Excel file for {state_name}, {rto_name}, {year}")
            
            # Find the Excel download button
            excel_button = self.wait_and_scroll_to_element(By.ID, "groupingTable:xls", 20, "Excel download button")
            
            if not excel_button:
                # Try alternative methods
                locators = [
                    (By.XPATH, "//button[contains(@id, 'xls')]"),
                    (By.XPATH, "//button[contains(@title, 'Excel')]"),
                    (By.CSS_SELECTOR, "button[id$='xls']")
                ]
                
                for locator_type, locator in locators:
                    excel_button = self.wait_and_scroll_to_element(locator_type, locator, 5, "Excel button alternative")
                    if excel_button:
                        break
            
            if not excel_button:
                self.log_message("Could not find Excel download button")
                return False
            
            # Update preferences for download directory - safer method than CDP
            # old_prefs = self.driver.execute_script('return window.navigator.userAgent;')
            
            # Click the download button
            self.smart_click(excel_button, "Excel download button")
            self.log_message("Clicked Excel download button")
            
            # Wait for download to complete
            safe_state_name = state_name.split('(')[0].strip().replace(' ', '_')
            safe_rto_name = re.sub(r'\s*\(\d{2}-[A-Z]{3}-\d{4}\)\s*$', '', rto_name).strip()
            target_dir = os.path.join(self.base_download_dir, state_name)
            self.random_delay(4, 5)
            os.makedirs(target_dir, exist_ok=True)
            
            # Call wait_for_download_and_rename with the year parameter
            result = self.wait_for_download_and_rename(target_dir, safe_state_name, safe_rto_name, year)
            return result
            
        except Exception as e:
            self.log_message(f"Error in download_excel_rto: {str(e)}")
            import traceback
            self.log_message(f"Traceback: {traceback.format_exc()}")
            return False
    
    def wait_for_download_and_rename(self, target_dir, state_name, rto_name, year):
        """
        Wait for download to complete, rename file, and upload to S3
        
        Args:
            target_dir (str): Directory to move the downloaded file to
            state_name (str): Name of the state for S3 path
            rto_name (str): Name of the RTO for filename
            year (int): Year for S3 path
            
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            self.log_message(f"BASE_DOWNLOAD_DIR being checked: {self.base_download_dir}")
            self.log_message(f"Inside_wait_for_download_and_rename for {state_name}")
            
            # Ensure the base download directory exists
            if not os.path.exists(self.base_download_dir):
                self.log_message(f"Creating base download directory: {self.base_download_dir}")
                os.makedirs(self.base_download_dir, exist_ok=True)
            
            found_file = None
            timeout = 60  # 60 second timeout for download
            start_time = time.time()
            temp_file_pattern = re.compile(r'^.*\.(crdownload|part|tmp)$', re.IGNORECASE)
    
            # Wait for download to complete
            while time.time() - start_time < timeout:
                try:
                    current_files = os.listdir(self.base_download_dir)
                    self.log_message(f"Current files in download dir: {current_files}")
                    
                    # Filter out temporary download files
                    valid_files = [
                        f for f in current_files 
                        if f.lower().endswith(('.xlsx', '.xls')) 
                        and not temp_file_pattern.match(f)
                    ]
                    
                    if valid_files:
                        # Get the most recently modified file
                        valid_files.sort(key=lambda f: os.path.getmtime(
                            os.path.join(self.base_download_dir, f)), reverse=True)
                        found_file = os.path.join(self.base_download_dir, valid_files[0])
                        # Small delay to ensure the file is completely written
                        time.sleep(2)
                        break
                            
                    time.sleep(1)  # wait and retry
                        
                except (OSError, Exception) as e:
                    self.log_message(f"Error checking files: {str(e)}")
                    time.sleep(1)
    
            if not found_file:
                self.log_message("Download timeout - file not found")
                return False
                
            try:
                # Ensure target directory exists
                os.makedirs(target_dir, exist_ok=True)
                self.log_message(f"Target directory verified/created: {target_dir}")
                
                # Create a unique filename if the target exists
                base_name = f"{rto_name}.xlsx"
                new_filepath = os.path.join(target_dir, base_name)
                
                # If file exists, append a number to make it unique
                counter = 1
                while os.path.exists(new_filepath):
                    name, ext = os.path.splitext(base_name)
                    new_filepath = os.path.join(target_dir, f"{name}_{counter}{ext}")
                    counter += 1
                
                # Move the file to target directory
                self.log_message(f"Moving file from {found_file} to {new_filepath}")
                os.rename(found_file, new_filepath)
                
                if not os.path.exists(new_filepath):
                    self.log_message("Error: File move operation failed")
                    return False
                    
                # Upload to S3
                s3_key = f"{self.s3_base_prefix}{state_name}/{year}/{os.path.basename(new_filepath)}"
                
                if self.upload_to_s3(new_filepath, s3_key):
                    self.log_message(f"Successfully uploaded to S3: s3://{self.s3_bucket_name}/{s3_key}")
                    
                    # Optionally remove local file after successful upload
                    try:
                        os.remove(new_filepath)
                        self.log_message(f"Removed local file: {new_filepath}")
                    except Exception as e:
                        self.log_message(f"Warning: Could not remove local file {new_filepath}: {str(e)}")
                    
                    return True
                else:
                    self.log_message("S3 upload failed, keeping local file")
                    return False
                    
            except Exception as e:
                self.log_message(f"Error processing downloaded file: {str(e)}")
                return False
                
        except Exception as e:
            self.log_message(f"Unexpected error in download wait: {str(e)}")
            return False
    
        except Exception as e:
            self.log_message(f"Unexpected error in download wait: {str(e)}")
            return False

    
    def process_rto_wise_data(self, state_name, year, specific_rtos=None):
        """Main function to process RTO-wise data for a state"""
        try:
            self.log_message(f"\n=== Starting RTO-wise processing for {state_name}, {year} ===")
            
            # Setup page
            if not self.setup_axis():
                return False
            
            self.random_delay(1, 2)

            # Select state
            if not self.select_state_primefaces(state_name):
                return False
            
            self.random_delay(0, 1)
            
            # Select year
            if not self.select_year(year):
                return False
            
            self.random_delay(0, 1)
            
            # Get list of RTOs for this state
            if specific_rtos is None:
                rto_list = self.get_all_rtos_for_state()
                if not rto_list:
                    self.log_message(f"No RTOs found for {state_name}")
                    return False
            else:
                rto_list = specific_rtos
            
            self.log_message(f"Processing {len(rto_list)} RTOs for {state_name}")
            
            successful_rtos = 0
            failed_rtos = 0

            # Process each RTO
            for rto in rto_list:
                self.log_message(f"\n--- Processing RTO: {rto} ---")
                
                try:
                    # Select specific RTO
                    if not self.select_specific_rto(rto):
                        self.log_message(f"Failed to select RTO: {rto}")
                        failed_rtos += 1
                        continue
                    
                    self.random_delay(0.5, 1)

                    # Apply EV filters
                    if not self.apply_ev_filters():
                        self.log_message(f"Failed to apply filters for RTO: {rto}")
                        failed_rtos += 1
                        continue

                    self.random_delay(1, 2)
                    
                    # Download data
                    if self.download_excel_rto(state_name, year, rto):
                        self.log_message(f"✓ Successfully processed RTO: {rto}")
                        successful_rtos += 1
                    else:
                        self.log_message(f"✗ Failed to download data for RTO: {rto}")
                        failed_rtos += 1
                    
                    # Small delay between RTOs
                    self.random_delay(2, 3)
                    
                except Exception as e:
                    self.log_message(f"Error processing RTO {rto}: {str(e)}")
                    failed_rtos += 1
                    continue
            
            # Summary
            self.log_message(f"\n=== RTO Processing Summary for {state_name} ===")
            self.log_message(f"Successful RTOs: {successful_rtos}")
            self.log_message(f"Failed RTOs: {failed_rtos}")
            self.log_message(f"Total RTOs: {len(rto_list)}")
            
            return successful_rtos > 0
            
        except Exception as e:
            self.log_message(f"Error in process_rto_wise_data: {str(e)}")
            return False
    
    def close(self):
        """Close the browser"""
        try:
            self.driver.quit()
            self.log_message("Browser closed successfully")
        except Exception as e:
            self.log_message(f"Error closing browser: {str(e)}")

# Usage Example
def main():
    processor = RTOWiseProcessor(base_download_dir=os.path.join(os.getcwd(), f"rto_2024"))
    
    try:
        # Configuration
        states_to_process = [
            "Himachal Pradesh(96)",
            "Karnataka(68)", "Assam(33)", "Bihar(48)",
            "Madhya Pradesh(53)", 
            "Chandigarh(1)", "Chhattisgarh(31)", "Delhi(16)",
            "Goa(13)", "Gujarat(37)", "Jammu and Kashmir(21)",
            "Jharkhand(25)", "Kerala(87)", "Ladakh(3)", "Lakshadweep(5)",
            "Manipur(13)", "Meghalaya(13)", "Mizoram(10)",
            "Nagaland(9)", "Odisha(39)", "Puducherry(8)", 
            "Uttarakhand(21)", 
            "UT of DNH and DD(3)", "Andaman & Nicobar Island(3)", "Sikkim(9)", "Tripura(9)"
        ]
        year = 2024
        
        # Process each state
        for state in states_to_process:
            success = processor.process_rto_wise_data(state, year)
            if success:
                processor.log_message(f"✓ Successfully completed {state}")
            else:
                processor.log_message(f"✗ Failed to process {state}")
            
            # Delay between states
            processor.random_delay(2, 3)
    
    finally:
        processor.close()

if __name__ == "__main__":
    main()