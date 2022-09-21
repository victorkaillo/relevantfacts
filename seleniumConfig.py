import os
from seleniumwire import webdriver
from selenium.webdriver.firefox.options import Options as OptionsFirefox
import traceback
from environs import Env


env = Env()
env.read_env()


def set_firefox_options() -> OptionsFirefox:
    firefox_options = OptionsFirefox()
    firefox_options.add_argument("--headless")
    firefox_options.add_argument("--no-sandbox")
    print(f'PASSOU!! set_firefox_options: {firefox_options}')
    return firefox_options

def set_firefox_profile() -> webdriver.FirefoxProfile:
    print('PASSOU!! set_firefox_profile')
    profile = webdriver.FirefoxProfile()

    profile.set_preference("browser.download.folderList", 2)

    profile.set_preference("browser.download.manager.showWhenStarting", False)

    profile.set_preference("browser.download.dir", os.path.join(os.path.dirname(__file__), 'downloads'))

    profile.set_preference("browser.helperApps.neverAsk.saveToDisk", "application/xls;text/csv;application/excel;application/vnd.ms-excel;application/x-excel;application/x-msexcel")

    return profile

def getDriver(driver = None):
    # try:
        if env.bool("DOCKER"):
            print(f'DOCKER={env.bool("DOCKER")}')
            driver = webdriver.Firefox(firefox_profile=set_firefox_profile(), options=set_firefox_options())
        else:
            print(f'DOCKER={env.bool("DOCKER")}')
            driver = webdriver.Firefox(executable_path='path/to/geckodriver')
        print(f'driver: {driver}')
        return driver
    # except Exception as err:
        # LogService.sendLog(success=False, job="seleniumConfig", source="kinvo.crawler.conta.azul",
        #     textMessage=str(err), level="CRITICAL", environment="PRODUCTION", details=traceback.format_exc())