from bs4 import BeautifulSoup# Soup object for HTML response
import requests #HTTP


def getResponse(url):
    """HTTP GET request"""
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/113.0.0.0 Safari/537.36"
        )
    }
    response = requests.get(url, headers=headers)
    if response.status_code != 200:
        raise Exception(f"Failed to retrieve data. HTTP Status code: {response.status_code}")
    #if response:
        #print("I got a response here in getResponse.")
    return BeautifulSoup(response.content, "html.parser")

def getData(url, html_id):
    """Gets attributes for equity, fixed income, and currencies"""
    soup_object=getResponse(url)

    #Add find statement if finding another html_attr)
    #coochribute= soup_object.find(html_attr, class_=html_id)

    try:
        coochribute= soup_object.find('span', html_id)
    except AttributeError:
        print("Attribute not found.")
    try:
        coochribute= soup_object.find('class', html_id)
    except AttributeError:
        print("Attribute not found.")
    """ try:
        coochribute= soup_object.find(html_attr, class_=html_id)
    except AttributeError:
        print("Attribute not found.")

 """
    return coochribute