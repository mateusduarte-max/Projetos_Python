from playwright.sync_api import sync_playwright

with sync_playwright() as p:
    browser = p.chromium.launch(headless=False)
    page = browser.new_page()
    #page.goto("https://suporte.integracommerce.com.br/support/login")
    page.goto("https://suporte.integracommerce.com.br/a/tickets/filters/220573")
    page.fill('#user_session_email', 'mateus.duarte@magazineluiza.com.br')
    page.fill('#user_session_password', 'Magazine2021')
    page.click('#new_user_session > fieldset > div:nth-child(4) > div > button')
    page.fill('#username', 'mateus.duarte@magazineluiza.com.br')
    page.fill('#password', 'mAGAZINE2022')
    '''page.click('#root > div.css-ldpdeo > div > div > div > div > div > div.css-9g5gyr > div.css-h05yjn-ContentContainer > form > div > div.css-aqlc50 > div > button')
    page.click('#ember19 > li:nth-child(2)')
    page.click('#burger-menu')
    page.fill('#ember2042 > svg', 'Erro_Anuncio_Dash_Mateus')'''



    #ember2049 > ul:nth-child(2) > li
    #page.screenshot(path="example.png")
    #page.screenshot(path="example.png")
    #browser.close()
    page.pause()