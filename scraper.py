import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

from collections import Counter
import hashlib
import json
import os
import atexit
import sys

website_fps = []
website_fps_wordcount = []  #track word count for each fingerprint

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def get_simhash_fingerprint(words: list) -> int:
    """
    Generates the fingerprint for the webpage content
    
    :param words: List of words from the page
    :return: The Simhash fingerprint for the webpage
    """
    shingle_len = 3
    vector = [0] * 64

    for i in range(len(words) - shingle_len + 1):
        shingle = " ".join(words[i:i+shingle_len])
        token_hash = int.from_bytes(hashlib.blake2b(shingle.encode(), digest_size=8).digest(), 'big')

        for i in range(64):
            bitmask = 1 << i

            if token_hash & bitmask:
                vector[i] += 1
            else:
                vector[i] -= 1

    fingerprint = 0

    for i in range(64):
        if vector[i] >= 0:
            fingerprint |= (1 << i)

    return fingerprint


def is_near_dup(page_words: list) -> bool:
    word_count = len(page_words)
    
    #only compare against pages with similar word count
    candidates = []
    for idx, stored_count in enumerate(website_fps_wordcount):
        if abs(word_count - stored_count) / max(word_count, stored_count, 1) <= 0.2:
            candidates.append(idx)
    
    #if many candidates, compute fingerprint and compare
    if candidates:
        curr_fingerprint = get_simhash_fingerprint(page_words)
        
        for idx in candidates:
            difference_distance = (curr_fingerprint ^ website_fps[idx]).bit_count()
            similarity = (64 - difference_distance) / 64
            
            if similarity >= 0.95:
                return True
    
    #store for future comparisons
    website_fps.append(get_simhash_fingerprint(page_words) if not candidates else curr_fingerprint)
    website_fps_wordcount.append(word_count)
    
    return False


def extract_next_links(url: str, resp):
    # Implementation required.
    # url: the URL that was used to get the page
    # resp.url: the actual url of the page
    # resp.status: the status code returned by the server. 200 is OK, you got the page. Other numbers mean that there was some kind of problem.
    # resp.error: when status is not 200, you can check the error here, if needed.
    # resp.raw_response: this is where the page actually is. More specifically, the raw_response has two parts:
    #         resp.raw_response.url: the url, again
    #         resp.raw_response.content: the content of the page!
    # Return a list with the hyperlinks (as strings) scrapped from resp.raw_response.content
    
    # If the webpage fetch fails or is empty, then just return, no links to extract.
    if (resp.status != 200 or not resp.raw_response or not resp.raw_response.content):
        return []
    
    # Setting up BS Object for page parsing
    bs_web = BeautifulSoup(resp.raw_response.content, "html.parser")
    page_text = bs_web.get_text()
    page_words = text_to_word(page_text)

    if resp.url not in seen_urls:
        update_statistics(resp.url, page_words)
        seen_urls.add(resp.url)
    
    if len(page_words) < 50:
        return []  # Don't crawl links from low content pages
    
    if is_near_dup(page_words):
        return []  # Don't crawl links from duplicate pages

    anchor_tags = bs_web.find_all('a', href=True)
    
    extract_links = [""] * len(anchor_tags)
    for i, anchor in enumerate(anchor_tags):
        raw_href = anchor.get('href')

        try:
            full_url = urljoin(resp.url, raw_href)
            extract_links[i] = urldefrag(full_url)[0]
        except Exception:
            continue
    
    return extract_links

def is_valid(url):
    # Decide whether to crawl this url or not. 
    # If you decide to crawl it, return True; otherwise return False.
    # There are already some conditions that return False.
    valid_depts = set(['ics', 'cs', 'informatics', 'stat'])
    
    try:
        parsed = urlparse(url)
        
        if parsed.scheme not in set(["http", "https"]):
            return False
    
        
        domains = parsed.netloc.split('.')

        if len(domains) < 3 or domains[-1] != 'edu' or domains[-2] != 'uci':
            return False
        
        has_valid_dept = False
        for dept in valid_depts:
            if dept in domains:
                has_valid_dept = True
                break
        
        if not has_valid_dept:
            return False
        
        # File extension filtering
        if re.match(
            r".*\.(css|can|mat|nc|bigw|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|mpg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|ppsx|doc|docx|xls|xlsx|names"
            + r"|data|dat|exe|bz2|tar|msi|bin|7z|psd|dmg|iso"
            + r"|epub|dll|cnf|tgz|sha1"
            + r"|thmx|mso|arff|rtf|jar|csv"
            + r"|rm|smil|wmv|swf|wma|zip|rar|gz)$", parsed.path.lower()):
            return False
        
        """
        Additional validation
        """
        
        """
        "Detect and avoid infinite traps"
        """

        # Avoid common dynamic table traps
        if re.search(r"(do|sortby|sortdir|rev|version|precision|from|diff|format|action)=", parsed.query):
            return False
        
        if re.search(r"/(timeline|search|changeset|attachment)", parsed.path.lower()):
            return False

        # Avoid long URLs (Limit trap)
        if len(url) > 200:
            return False
            
        # Avoid too many path segments
        pathSegments = []
        for seg in parsed.path.split('/'):
            if seg != '':
                pathSegments.append(seg)
        if len(pathSegments) > 10:
            return False
    
        
        # Detect repeating path patterns
        if len(pathSegments) > 3:
            segmentCounts = {}
            for seg in pathSegments:
                segmentCounts[seg] = segmentCounts.get(seg, 0) + 1
                if segmentCounts[seg] > 2:
                    return False
        
        
        # Avoid common trap patterns
        trapPatterns = [
            r'/calendar/',
            r'/event/',
            r'/gallery/',
            r'/img_\d+',
            r'/photo/',
            r'/filter/',
            r'/share\?',
            r'/print\?',
            r'/attachment/',
        ]
        for pattern in trapPatterns:
            if re.search(pattern, parsed.path.lower()):
                return False

        
        # Detect and avoid session IDs in URLs
        if re.search(r"(sessionid|sid|phpsessid|jsessionid|aspsessionid|sessid)=[a-zA-Z0-9]+", url.lower()):
            return False
    
        
        # Avoid common low information pages
        low_info_patterns = [
            r'/login',
            r'/logout',
            r'/register',
            r'/auth',
            r'/admin',
            r'/feed/',
            r'/download/',
        ]
        for pattern in low_info_patterns:
            if re.search(pattern, parsed.path.lower()):
                return False
        
        # Avoid repeating date patterns in URLs
        if re.search(r'(/\d{4}){2,}', parsed.path) or re.search(r'(/\d{2}){3,}', parsed.path):
            return False

        # Avoid URLs with excessive repeating characters
        if re.search(r'(.)\1{5,}', parsed.path):
            return False
        return True
        
    except TypeError:
        print ("TypeError for ", parsed)
        raise



#Void function to convert a bs object to statistics, via writing to a file permanently. 
#unique page count
#longest page, given by link
#50 most common words, given by freq. Ignore stop words. 
#All subdomains found in uci.edu, listed alphabetically, then count of new pages.
#(vision.ics.uci.edu, 10)


#########################
#STATS GLOBAL VARIABLES
STATS_JSON_FILE = "crawler_stats.json"
unique_page_count = 0
longest_page_link = ""
longest_page_length = -1
most_common_words = Counter()
sub_domain_pages = {}
seen_urls = set()
########################

def load_statistics():
    """load statistics from JSON file"""
    global unique_page_count, longest_page_length, longest_page_link, most_common_words, sub_domain_pages, seen_urls
    
    # Check if --restart was passed as command line argument
    is_restart = '--restart' in sys.argv
    
    if is_restart and os.path.exists(STATS_JSON_FILE):
        print("Clean restart requested - clearing old statistics")
        os.remove(STATS_JSON_FILE)
        print("Starting with fresh statistics")
        return
    
    if os.path.exists(STATS_JSON_FILE):
        try:
            with open(STATS_JSON_FILE, 'r') as f:
                data = json.load(f)
                unique_page_count = data.get('unique_page_count', 0)
                longest_page_length = data.get('longest_page_length', -1)
                longest_page_link = data.get('longest_page_link', "")
                most_common_words = Counter(data.get('most_common_words', {}))
                # Convert list of URLs back to sets for each subdomain
                sub_domain_pages = {k: set(v) for k, v in data.get('sub_domain_pages', {}).items()}
            print(f"Resuming from previous run: {unique_page_count} pages, {len(sub_domain_pages)} subdomains")
        except Exception as e:
            print(f"Warning: Failed to load stats: {e}, starting from scratch")
    else:
        print("No existing stats file found, starting fresh")

def save_statistics():
    """save all statistics to JSON"""
    try:
        data = {
            'unique_page_count': unique_page_count,
            'longest_page_length': longest_page_length,
            'longest_page_link': longest_page_link,
            'most_common_words': dict(most_common_words.most_common(50)),  # Save only top 50 words
            'sub_domain_pages': {k: list(v) for k, v in sub_domain_pages.items()},  # Convert sets to lists for JSON
        }
        with open(STATS_JSON_FILE, 'w') as f:
            json.dump(data, f, indent=2)
    except Exception as e:
        print(f"Failed to save stats: {e}")

#load existing stats when module is imported
try:
    load_statistics()
except Exception as e:
    print(f"Error during stats loading: {e}, continuing anyway")

def update_statistics(url: str, tokens: list) -> None:
    global unique_page_count, longest_page_length, longest_page_link, most_common_words, sub_domain_pages
    unique_page_count += 1
    
    if len(tokens) >= 50:
        if len(tokens) > longest_page_length:
            longest_page_link = url
            longest_page_length = len(tokens)

        most_common_words.update(
            token for token in tokens 
            if token not in stop_words 
            and len(token) > 2
            and not token.isnumeric() 
            )
    else:
        #low content page: counted as unique but not analyzed for words
        pass

    #validate it's in the allowed set
    hostname = urlparse(url).hostname
    if hostname is not None:
        # Remove www.
        if hostname.startswith("www."):
            hostname = hostname[4:]
        
        #valid patterns: ics.uci.edu, cs.uci.edu, informatics.uci.edu, stat.uci.edu
        #and any subdomain like vision.ics.uci.edu, etc.
        valid_depts = ['ics.uci.edu', 'cs.uci.edu', 'informatics.uci.edu', 'stat.uci.edu']
        is_valid_subdomain = False
        
        for dept in valid_depts:
            if hostname == dept or hostname.endswith('.' + dept):
                is_valid_subdomain = True
                break
        
        if is_valid_subdomain:
            if hostname not in sub_domain_pages:
                sub_domain_pages[hostname] = set()
            sub_domain_pages[hostname].add(url)
    
    #save statistics every 50 pages - to survive crashes
    if unique_page_count % 50 == 0:
        save_statistics()

#Helper functions

stop_words = {
    "a", "about", "above", "after", "again", "against", "all", "am", "an", "and", "any", "are", "aren't", "as", "at",
    "be", "because", "been", "before", "being", "below", "between", "both", "but", "by", "can't", "cannot", "could",
    "couldn't", "did", "didn't", "do", "does", "doesn't", "doing", "don't", "down", "during", "each", "few", "for",
    "from", "further", "had", "hadn't", "has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's",
    "her", "here", "here's", "hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm",
    "i've", "if", "in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
    "my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our", "ours",
    "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's", "should", "shouldn't", "so",
    "some", "such", "than", "that", "that's", "the", "their", "theirs", "them", "themselves", "then", "there", "there's",
    "these", "they", "they'd", "they'll", "they're", "they've", "this", "those", "through", "to", "too", "under", "until",
    "up", "very", "was", "wasn't", "we", "we'd", "we'll", "we're", "we've", "were", "weren't", "what", "what's", "when",
    "when's", "where", "where's", "which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would",
    "wouldn't", "you", "you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"
}
def text_to_word(text):
    return re.findall(r"[a-z]+(?:'[a-z]+)?", text.lower())

def final_report():
    global unique_page_count, longest_page_length, longest_page_link, most_common_words, sub_domain_pages
    
    #save current state to JSON first
    save_statistics()
    
    #generate final formatted report
    subdomains = sorted((domain, len(url_set)) for domain, url_set in sub_domain_pages.items())
    fifty_most_common_words = most_common_words.most_common(50)
    with open("stats.txt", 'w') as f:
        f.write(f"{unique_page_count}\n")
        f.write(f"{longest_page_link}\n")
        for word, count in fifty_most_common_words:
            f.write("{}: {} \n".format(word, count))
        for domain, count in subdomains:
            f.write("{}, {}\n".format(domain, count))
    
    print(f"Unique pages: {unique_page_count}")
    print(f"Subdomains found: {len(subdomains)}")
    print(f"Total unique words: {len(most_common_words)}")

atexit.register(save_statistics)
atexit.register(final_report)

