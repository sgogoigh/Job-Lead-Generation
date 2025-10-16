import re
import time
from urllib.parse import urlparse, urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup
from duckduckgo_search import DDGS
from dateutil import parser as dateparser
from tqdm import tqdm

INPUT_CSV = "Growth For Impact Data Assignment_Data.csv"
OUTPUT_XLSX = "company_enrichment_output_fixed.xlsx"
USER_AGENT = "Mozilla/5.0 (compatible; CompanyEnricher/1.0; +https://example.com/bot)"
REQUESTS_TIMEOUT = 15
SLEEP_BETWEEN_REQUESTS = 1.0
MAX_JOBS_PER_COMPANY = 3
MAX_SEARCH_RESULTS = 10

ATS_PROVIDERS = [
    ("teamtailor", "teamtailor.com"),
    ("lever", "lever.co"),
    ("greenhouse", "greenhouse.io"),
    ("workable", "apply.workable.com"),
    ("personio", "personio.com"),
    ("zohorecruit", "zohorecruit.com"),
    ("smartrecruiters", "smartrecruiters.com"),
    ("workday", "workday.com"),
]

HEADERS = {"User-Agent": USER_AGENT}

def safe_get(url, params=None, headers=None, retries=2):
    headers = headers or HEADERS
    for attempt in range(retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=REQUESTS_TIMEOUT, headers=headers)
            return resp
        except Exception:
            if attempt < retries:
                time.sleep(1 + attempt * 2)
            else:
                return None


def normalize_url(u):
    if pd.isna(u) or not u:
        return ""
    u = str(u).strip()
    if not u:
        return ""
    if not re.match(r"^https?://", u):
        u = "https://" + u
    return u.rstrip("/")


def domain_of(url):
    try:
        return urlparse(url).netloc.lower().replace("www.", "")
    except Exception:
        return ""


def pick_best_site(results, company_name):
    if not results:
        return None
    cname = re.sub(r"[^a-z0-9]+", " ", company_name.lower())
    tokens = [t for t in cname.split() if t and len(t) > 2]
    scored = []
    for url in results:
        d = domain_of(url)
        score = 0
        for t in tokens:
            if t in d:
                score += 3
        pathlen = len(urlparse(url).path.strip("/").split("/"))
        score -= min(pathlen, 3)
        if re.search(r"\.(com|org|net|io|co|gov|edu)$", d):
            score += 1
        scored.append((score, url))
    scored.sort(reverse=True)
    return scored[0][1]


def search_duckduckgo(query, max_results=MAX_SEARCH_RESULTS):
    urls = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results):
                # r is a dict, keys vary: 'href', 'url', 'link' appear in different versions
                u = r.get("href") or r.get("url") or r.get("link") or r.get("href")
                if u:
                    urls.append(u)
    except Exception:
        # Silent fallback to empty list
        pass
    return urls


def find_official_site(company_name, company_description):
    q1 = f"{company_name} official website"
    results = search_duckduckgo(q1)
    if results:
        best = pick_best_site(results, company_name)
        if best:
            return normalize_url(best)
    results = search_duckduckgo(company_name)
    if results:
        best = pick_best_site(results, company_name)
        if best:
            return normalize_url(best)
    return ""


def find_linkedin_company(company_name):
    q = f"site:linkedin.com/company {company_name}"
    results = search_duckduckgo(q, max_results=6)
    for u in results:
        if "linkedin.com/company" in u:
            return normalize_url(u)
    return ""


def find_careers_on_domain(domain_url):
    if not domain_url:
        return ""
    candidates = [
        "/careers",
        "/jobs",
        "/about/careers",
        "/company/careers",
        "/careers-us",
        "/join-us",
        "/work-with-us",
        "/join-our-team",
    ]
    root = normalize_url(domain_url)
    if not root:
        return ""
    for c in candidates:
        url = root.rstrip("/") + c
        resp = safe_get(url)
        if resp and resp.status_code == 200 and len(resp.text) > 200:
            return normalize_url(url)
    # fallback: search "site:domain careers"
    q = f"site:{domain_of(root)} careers OR jobs"
    results = search_duckduckgo(q, max_results=6)
    for u in results:
        if domain_of(u) == domain_of(root) and re.search(r"caree|job|join", u, re.I):
            return normalize_url(u)
    return ""


def detect_ats_from_search(company_name):
    ats_hits = {}
    for label, host in ATS_PROVIDERS:
        q = f"site:{host} {company_name}"
        results = search_duckduckgo(q, max_results=5)
        for u in results:
            if host in u:
                ats_hits[label] = normalize_url(u)
                break
    return ats_hits

def parse_teamtailor_listings(listing_url):
    out = []
    resp = safe_get(listing_url)
    if not resp or resp.status_code != 200:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select("a[href*='/jobs/']")
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(listing_url, href)
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        out.append({"url": normalize_url(href), "title": title})
        if len(out) >= MAX_JOBS_PER_COMPANY:
            break
    return out


def parse_lever_listings(listing_url):
    out = []
    resp = safe_get(listing_url)
    if not resp or resp.status_code != 200:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select("a[href*='/jobs/']")
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(listing_url, href)
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        out.append({"url": normalize_url(href), "title": title})
        if len(out) >= MAX_JOBS_PER_COMPANY:
            break
    return out


def parse_greenhouse_listings(listing_url):
    out = []
    resp = safe_get(listing_url)
    if not resp or resp.status_code != 200:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select("a[href*='/jobs/'], div.opening a")
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(listing_url, href)
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        out.append({"url": normalize_url(href), "title": title})
        if len(out) >= MAX_JOBS_PER_COMPANY:
            break
    return out


def parse_workable_listings(listing_url):
    out = []
    resp = safe_get(listing_url)
    if not resp or resp.status_code != 200:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select("a[href*='/jobs/']")
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(listing_url, href)
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        out.append({"url": normalize_url(href), "title": title})
        if len(out) >= MAX_JOBS_PER_COMPANY:
            break
    return out


def parse_personio_listings(listing_url):
    out = []
    resp = safe_get(listing_url)
    if not resp or resp.status_code != 200:
        return out
    soup = BeautifulSoup(resp.text, "html.parser")
    anchors = soup.select("a[href*='/job/'], a[href*='/jobs/']")
    seen = set()
    for a in anchors:
        href = a.get("href")
        if not href:
            continue
        if href.startswith("/"):
            href = urljoin(listing_url, href)
        if href in seen:
            continue
        seen.add(href)
        title = a.get_text(strip=True)
        out.append({"url": normalize_url(href), "title": title})
        if len(out) >= MAX_JOBS_PER_COMPANY:
            break
    return out


def extract_job_details(job_url):
    resp = safe_get(job_url)
    if not resp or resp.status_code != 200:
        return {"title": "", "location": "", "date": "", "snippet": ""}
    soup = BeautifulSoup(resp.text, "html.parser")
    title = ""
    if soup.title and soup.title.string:
        title = soup.title.string.strip()
    h1 = soup.find("h1")
    if h1 and h1.get_text(strip=True):
        title = h1.get_text(strip=True)
    location = ""
    loc_candidates = soup.find_all(text=re.compile(r"Location|location|City|city", re.I))
    for t in loc_candidates:
        parent = t.parent
        if parent and parent.next_sibling:
            ls = str(parent.next_sibling)
            if len(ls) < 200:
                location = re.sub(r"\s+", " ", BeautifulSoup(ls, "html.parser").get_text()).strip()
                break
    date_text = ""
    date_candidates = soup.find_all(text=re.compile(r"Posted|posted|Date|date|Published|published", re.I))
    for t in date_candidates:
        s = t.parent.get_text(" ", strip=True)
        m = re.search(r"(\b\d{1,2}\s+\w+\s+\d{4}\b|\b\w+\s+\d{1,2},\s*\d{4}\b|\b\d{4}-\d{2}-\d{2}\b)", s)
        if m:
            date_text = m.group(0)
            break
    main_text = ""
    for sel in ["div.job-description", "div.description", "section.job", "div#job", "article", "div[class*='description']"]:
        node = soup.select_one(sel)
        if node:
            main_text = node.get_text(" ", strip=True)
            break
    if not main_text:
        texts = [t.get_text(" ", strip=True) for t in soup.find_all(["p", "div"]) if len(t.get_text(strip=True)) > 50]
        if texts:
            main_text = max(texts, key=len)
    snippet = (main_text[:500] + "...") if main_text else ""
    date_iso = ""
    if date_text:
        try:
            date_iso = dateparser.parse(date_text, fuzzy=True).date().isoformat()
        except Exception:
            date_iso = date_text
    return {"title": title, "location": location, "date": date_iso, "snippet": snippet}

def enrich_companies(df):
    out_df = df.copy()

    # Ensure basic columns exist
    base_cols = ["Website URL", "Linkedin URL", "Careers Page URL", "Job listings page URL"]
    for col in base_cols:
        if col not in out_df.columns:
            out_df[col] = ""

    # create job_post columns if missing
    for i in range(1, MAX_JOBS_PER_COMPANY + 1):
        for suffix in ["url", "title", "location", "date", "snippet"]:
            colname = f"job_post{i}_{suffix}"
            if colname not in out_df.columns:
                out_df[colname] = ""

    total_jobs_found = 0

    for idx, row in tqdm(out_df.iterrows(), total=len(out_df), desc="Enriching companies"):
        company = str(row.get("Company Name", "")).strip()
        descr = str(row.get("Company Description", "")).strip()
        if not company:
            continue

        website = normalize_url(row.get("Website URL", "")) or ""
        linkedin = normalize_url(row.get("Linkedin URL", "")) or ""
        careers = normalize_url(row.get("Careers Page URL", "")) or ""
        joblist = normalize_url(row.get("Job listings page URL", "")) or ""

        if not website:
            website = find_official_site(company, descr)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        out_df.at[idx, "Website URL"] = website

        if not linkedin:
            linkedin = find_linkedin_company(company)
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        out_df.at[idx, "Linkedin URL"] = linkedin

        if not careers:
            if website:
                careers = find_careers_on_domain(website)
            if not careers:
                careers_search = search_duckduckgo(f"{company} careers", max_results=6)
                for u in careers_search:
                    if re.search(r"caree|job|join", u, re.I):
                        careers = normalize_url(u)
                        break
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        out_df.at[idx, "Careers Page URL"] = careers

        if not joblist:
            ats_hits = detect_ats_from_search(company)
            if ats_hits:
                priority = ["teamtailor", "lever", "greenhouse", "workable", "personio", "zohorecruit", "smartrecruiters"]
                chosen = ""
                for p in priority:
                    if p in ats_hits:
                        chosen = ats_hits[p]
                        break
                if not chosen:
                    chosen = next(iter(ats_hits.values()))
                joblist = chosen
            else:
                if careers and any(x in careers.lower() for x in ["careers", "jobs", "join"]):
                    joblist = careers
                else:
                    search_res = search_duckduckgo(f"{company} jobs", max_results=6)
                    for u in search_res:
                        if re.search(r"teamtailor|lever.co|greenhouse.io|workable.com|personio.com|zohorecruit.com|smartrecruiters.com", u, re.I):
                            joblist = normalize_url(u)
                            break
            time.sleep(SLEEP_BETWEEN_REQUESTS)
        out_df.at[idx, "Job listings page URL"] = joblist

        jobs_to_write = []
        if joblist:
            try:
                host = urlparse(joblist).netloc.lower()
                if "teamtailor" in host:
                    jobs_to_write = parse_teamtailor_listings(joblist)
                elif "lever.co" in host or "lever" in joblist:
                    jobs_to_write = parse_lever_listings(joblist)
                elif "greenhouse" in host or "greenhouse" in joblist:
                    jobs_to_write = parse_greenhouse_listings(joblist)
                elif "workable" in host or "apply.workable.com" in host:
                    jobs_to_write = parse_workable_listings(joblist)
                elif "personio" in host:
                    jobs_to_write = parse_personio_listings(joblist)
                else:
                    resp = safe_get(joblist)
                    if resp and resp.status_code == 200:
                        soup = BeautifulSoup(resp.text, "html.parser")
                        anchors = soup.select("a[href*='/jobs/'], a[href*='/careers/'], a[href*='/job/']")
                        seen = set()
                        for a in anchors:
                            href = a.get("href")
                            if not href:
                                continue
                            if href.startswith("/"):
                                href = urljoin(joblist, href)
                            if href in seen:
                                continue
                            seen.add(href)
                            title = a.get_text(strip=True) or ""
                            jobs_to_write.append({"url": normalize_url(href), "title": title})
                            if len(jobs_to_write) >= MAX_JOBS_PER_COMPANY:
                                break
            except Exception:
                pass

        c = 1
        for job in jobs_to_write[:MAX_JOBS_PER_COMPANY]:
            jurl = job.get("url") or ""
            jtitle_guess = job.get("title") or ""
            details = extract_job_details(jurl) if jurl else {"title": jtitle_guess, "location": "", "date": "", "snippet": ""}
            out_df.at[idx, f"job_post{c}_url"] = jurl
            out_df.at[idx, f"job_post{c}_title"] = details.get("title") or jtitle_guess
            out_df.at[idx, f"job_post{c}_location"] = details.get("location", "")
            out_df.at[idx, f"job_post{c}_date"] = details.get("date", "")
            out_df.at[idx, f"job_post{c}_snippet"] = details.get("snippet", "")
            c += 1
            total_jobs_found += 1
            time.sleep(0.5)

        time.sleep(SLEEP_BETWEEN_REQUESTS)

    return out_df, total_jobs_found

def main():
    print("Loading input CSV:", INPUT_CSV)
    df = pd.read_csv(INPUT_CSV)
    if "Company Name" not in df.columns or "Company Description" not in df.columns:
        raise ValueError("Input CSV must contain 'Company Name' and 'Company Description' columns.")
    print("Starting enrichment for", len(df), "companies.")
    enriched_df, total_jobs = enrich_companies(df)

    methodology_text = (
        "Methodology:\n"
        "- Discovery via DuckDuckGo (DDGS().text()).\n"
        "- ATS detection + provider-specific parsing (Teamtailor, Lever, Greenhouse, Workable, Personio).\n"
        f"- Script run date: {pd.Timestamp.utcnow()}\n"
        "- Notes: dynamic JS pages may require Playwright/Selenium; use proxies if running large batches.\n"
    )

    with pd.ExcelWriter(OUTPUT_XLSX, engine="openpyxl") as writer:
        enriched_df.to_excel(writer, sheet_name="Data", index=False)
        pd.DataFrame({"Methodology": [methodology_text]}).to_excel(writer, sheet_name="Methodology", index=False)

    print("Enrichment complete. Jobs found:", total_jobs)
    print("Saved to:", OUTPUT_XLSX)


if __name__ == "__main__":
    main()