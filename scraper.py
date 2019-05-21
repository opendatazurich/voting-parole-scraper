# -*- coding: utf-8 -*-

import requests
from bs4 import BeautifulSoup
import sqlite3
import re
from urllib.parse import urljoin
import traceback

DATABASE_NAME = 'data.sqlite'
conn = sqlite3.connect(DATABASE_NAME)
c = conn.cursor()
c.execute('DROP TABLE IF EXISTS data')
c.execute(
    '''
    CREATE TABLE data (
        vote_name text,
        vote_level text,
        vote_date text,
        party text,
        party_level text,
        parole text
    )
    '''
)
conn.commit()


def find_parent(elem, tag_name):
    for parent in elem.parents:
            headers = parent.find(tag_name)
            if not headers:
                continue
            return headers


def find_vote_name(elem, tag_name, ignore):
    if not re.search(ignore, elem.text):
        return elem
    
    results = elem.find_all_previous(tag_name)
    for res in results:
        if not re.search(ignore, res.text):
            return res


def parse_parole_page(content, cursor):
    soup = BeautifulSoup(content, 'html.parser')
    
    tables = soup.find_all('table')
    ignore = re.compile("(.*Parteiparolen.*)|(.*Ergänzende Informationen.*)")
    
    votes = []
    for t in tables:
        # find parent header
        header = find_vote_name(find_parent(t, 'h2'), 'h2', ignore)
        vote_name = header.text.strip()
        print("")
        print(vote_name)
        
        paroles = []
        for row in t.find_all('tr'):
            party = row.find('th')
            parole = row.find('td')
            
            if party and parole:
                party_text = party.text.strip()
                parole_text = parole.text.strip()
                if party_text.lower() == 'glp':
                    party_text = party_text.lower()
                
                print("%s: %s" % (party.text.strip(), parole.text.strip()))
                
                parole = {
                    'party': party.text.strip(),
                    'party_level': 'communal',
                    'parole': parole.text.strip()
                }
                paroles.append(parole)
        
        vote = {
            'vote_level': 'Stadt Zürich',
            'vote_name': vote_name,
            'paroles': paroles,
        }
        votes.append(vote)
        
    return votes
    




# city of zurich
start_url = 'https://www.stadt-zuerich.ch/portal/de/index/politik_u_recht/abstimmungen_u_wahlen.html'

# check paroles of previous dates
page = requests.get(start_url)
soup = BeautifulSoup(page.content, 'html.parser')
prev_dates_link = soup.find_all("a", href=re.compile("[^#]+.*"), string=re.compile(".*Vergangene Termine.*"))
prev_url = urljoin(start_url, prev_dates_link[0]['href'])



prev_page = requests.get(prev_url)
soup = BeautifulSoup(prev_page.content, 'html.parser')

vote_links = soup.select('.mainparsys ul.linklist a')

#for vote_link in vote_links[:2]:
try:
    for vote_link in vote_links:
        vote_date = vote_link.text.strip()
        print("")
        print("")
        print(vote_date)
        vote_url = urljoin(prev_url, vote_link['href'])
        vote_page = requests.get(vote_url)
        soup = BeautifulSoup(vote_page.content, 'html.parser')
        city_vote = soup.find_all(string="Gemeindeabstimmung")
        if city_vote:
            voting_parole_link = soup.find("a", string=re.compile(".*(p|P)arole.*"))
            if not voting_parole_link:
                print("No voting parole link found")
                continue
            parole_url = urljoin(vote_url, voting_parole_link['href'])
            parole_page = requests.get(parole_url)
            
            votes = parse_parole_page(parole_page.content, c)
            for vote in votes:
                for parole in vote['paroles']:
                    c.execute(
                        '''
                        INSERT INTO data (
                            vote_name,
                            vote_level,
                            vote_date,
                            party,
                            party_level,
                            parole
                        )
                        VALUES
                        (?,?,?,?,?,?)
                        ''',
                        [
                            vote['vote_name'],
                            vote['vote_level'],
                            vote_date,
                            parole['party'],
                            parole['party_level'],
                            parole['parole'],
                        ]
                    )
            conn.commit()
except Exception as e:
    print("Error: %s" % e)
    print(traceback.format_exc())
finally:
    conn.close()