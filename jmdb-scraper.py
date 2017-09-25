from bs4 import BeautifulSoup
import couchdb
import requests
import time


COUCH_SERVER = ""
COUCH_DB = "jmdb"
ENCODING = "shift_jisx0213"


def getUrlAsSoup(url):
    """
    Returns url html text as beautiful soup object, decoded from shif_jisx0213
    """
    while True:
        try:
            r = requests.get(url)
        except:
            continue
        else:
            soup = BeautifulSoup(r.content.decode(ENCODING),"lxml")
            return soup

def getYearList(start, end):
    """
    Returns the complete list of movies from the Japanese movie database for a defined period
    """
    movie_list = []

    for year in range(start, end+1):
        soup = getUrlAsSoup("http://www.jmdb.ne.jp/{}/a{}.htm".format(year,year))
        print(soup)
        for movie in soup.find_all("li"):
            link = movie.find("a")
            date_string = link.text.split(" ")[0]
            date = date_string.split(".")
            year = int(date[0])
            try:
                month = int(date[1])
            except:
                month = 0
            try:
                day = int(date[2])
            except:
                day = 0
            url = "http://www.jmdb.ne.jp{}".format(link["href"][2:len(link["href"])])
            title = link.text.replace(date_string,"").strip()

            movie_list.append({
                "title": title,
                "year": year,
                "month": month,
                "day": day,
                "url": url
            })
        return movie_list


def getMovieInfos(url):
    """
    Gather additional movie information from url
    """
    soup = getUrlAsSoup(url)
    staff = []
    cast = []
    production = []
    distribution = ""
    info = ""#scrapes additional for a movie

    #check if movie site exists
    if "404" not in soup.find("title").text:

        #extract production infos
        production_full = soup.find("h2").next_element.next_element.strip()
        production = production_full.split("　")[0]
        if len(production.split("＝")) > 1:
            production = production.split("＝")[1:len(production.split("＝"))]
        else:
             production = [production]
        if len(production_full.split("　")) > 1:
            distribution = production_full.split("　")[1].replace("配給＝","")
        else:
            distribution = ""
        info = soup.find("a").next_element.next_element.next_element.next_element.strip()

        #extract cast and staff/crew
        isCast = False
        for row in soup.find_all("tr"):
            role = row.find_all("td")[0].text


            if role == "出演" or role == "配役":
                isCast = True

            for link in row.find_all("a"):
                url = "http://www.jmdb.ne.jp{}".format(link["href"][2:len(link["href"])])
                name = link.text

                if not isCast:
                    staff.append({
                        "role": role,
                        "name": name,
                        "url": url#scrapes additional for a movie
                    })
                else:
                    cast.append({
                        "role": role,
                        "name": name,
                        "url": url
                    })

        #print(role.text)

    return {
        "production": production,
        "distribution": distribution,
        "info": info,
        "staff": staff,
        "cast": cast
    }

def saveMovieListToDb(movie_list):
    couch = couchdb.Server()
    db = couch["jmdb"]
    for movie in movie_list:
        db.save(movie)
        print("movie {} saved in db".format(movie["title"]))

#iterate trough movies in couchdb and scrape details (production infos and cast/crew) for each movie
couch = couchdb.Server()
db = couch["jmdb"]
i = 0
for doc in db:
    i += 1
    movie = db[doc]
    if "staff" not in movie:
        if "url" in movie:
            print(i)

            print(movie["title"])
            infos = getMovieInfos(movie["url"])
            time.sleep(1)
            movie["production"] = infos["production"]
            movie["distribution"] = infos["distribution"]
            movie["info"] = infos["info"]
            movie["cast"] = infos["cast"]
            movie["staff"] = infos["staff"]
            db.save(movie)
            print("  ... updated")
            