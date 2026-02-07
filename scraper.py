import re
from urllib.parse import urlparse, urljoin, urldefrag
from bs4 import BeautifulSoup

from collections import Counter
import hashlib

website_fps = []

def scraper(url, resp):
    links = extract_next_links(url, resp)
    return [link for link in links if is_valid(link)]


def get_simhash_fingerprint(web_content: str) -> int:
    """
    Generates the fingerprint for the webpage content
    
    :param web_content: The webpage content
    :return: The Simhash fingerprint for the webpage
    """
    shingle_len = 3
    shingle_tokens = (web_content[i:i+shingle_len] for i in range(len(web_content) - (shingle_len - 1)))

    vector = [0] * 64

    for shingle_token in shingle_tokens:
        token_hash = int.from_bytes(hashlib.blake2b(shingle_token.encode(), digest_size=8).digest(), 'big')

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


def is_near_dup(page_content: str) -> int:
    curr_fingerprint = get_simhash_fingerprint(page_content)

    for fingerprint in website_fps:
        difference_distance = (curr_fingerprint ^ fingerprint).bit_count()

        similarity = (64 - difference_distance) / 64

        if similarity >= 0.95:
            return True
        
    website_fps.append(curr_fingerprint)

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

    if len(page_text.split()) < 50:
        return []
    
    # Update the statistics from the crawling session
    update_statistics(bs_web, resp.url)

    # Detect duplicate/similar links and filter out invalid links
    if is_near_dup(page_text):
        return []

    anchor_tags = bs_web.find_all('a', href=True)
    
    extract_links = [""] * len(anchor_tags)
    for i, anchor in enumerate(anchor_tags):
        raw_href = anchor.get('href')

        full_url = urljoin(resp.url, raw_href)
        extract_links[i] = urldefrag(full_url)[0]
    
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
        if domains[-1] != 'edu' or domains[-2] != 'uci' or domains[-3] not in valid_depts:
            return False

        # File extension filtering
        if re.match(
            r".*\.(css|js|bmp|gif|jpe?g|ico"
            + r"|png|tiff?|mid|mp2|mp3|mp4"
            + r"|wav|avi|mov|mpeg|ram|m4v|mkv|ogg|ogv|pdf"
            + r"|ps|eps|tex|ppt|pptx|doc|docx|xls|xlsx|names"
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
            if re.match(pattern, url.lower()):
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
            if pattern in url.lower():
                return False
        
        # Avoid repeating date patterns in URLs
        if re.search(r'(/\d{4}){2,}', parsed.path) or re.search(r'(/\d{2}){3,}', parsed.path):
            return False

        # Avoid URLs with excessive repeating characters
        if re.search(r'(.)\1{5,}', parsed.path):
            return False
        
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
unique_page_count = 0
longest_page_link = ""
longest_page_length = -1
most_common_words = Counter()
sub_domain_list = {}
########################
# BeautifulSoup(httpresp) -> reddit.com/IATA/somepost
def update_statistics(bs: BeautifulSoup, url: str) -> None:
    global unique_page_count, longest_page_length, longest_page_link, most_common_words, sub_domain_list
    unique_page_count += 1
    curr_tokens = text_to_word(bs.get_text())
    curr_length = len(curr_tokens)
    
    if(curr_length > longest_page_length):
        longest_page_link = url
        longest_page_length = curr_length

    most_common_words.update((token for token in curr_tokens if not token in stop_words and len(token) > 1))

    url_new = urlparse(url).hostname

    if url_new is not None and url_new[0:4] == "www.":
        url_new = url_new[4:]

    if url_new is not None and "uci.edu" in url_new:
        if url_new not in sub_domain_list:
            sub_domain_list[url_new] = 1
        else:
            sub_domain_list[url_new] +=1
    
    

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
    return re.findall(r"[a-zA-Z0-9]+(?:'[a-z]+)?", text.lower())

def final_report():
    global unique_page_count, longest_page_length, longest_page_link, most_common_words, sub_domain_list
    subdomains = sorted(sub_domain_list.items())
    fifty_most_common_words = most_common_words.most_common(50)
    with open("stats.txt", 'w') as f:
        f.write(f"{unique_page_count}\n")
        f.write(f"{longest_page_link}\n")
        for word, count in fifty_most_common_words:
            f.write("{}: {} \n".format(word, count))
        for domain, count in subdomains:
            f.write("{}, {}\n".format(domain, count))

