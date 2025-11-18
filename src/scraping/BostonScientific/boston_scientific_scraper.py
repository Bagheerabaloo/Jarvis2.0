import re
import csv
from typing import List, Dict, Any
from time import sleep

from selenium.webdriver.common.by import By
from selenium.common.exceptions import NoSuchElementException, StaleElementReferenceException


from src.common.web_driver.FirefoxDriver import FirefoxDriver  # cambia se hai un path diverso


START_URL = (
    "https://bostonscientific.eightfold.ai/careers"
    "?start=0&pid=563602809553908&sort_by=hot"
)


def _get_text_safe(element) -> str | None:
    """Return element.text stripped or None if element is None."""
    if element is None:
        return None
    txt = element.text
    return txt.strip() if txt is not None else None


def _to_csv_cell(value):
    """Normalize values for CSV: lists become a pipe-separated string."""
    if isinstance(value, list):
        return " | ".join(str(v) for v in value if v is not None)
    return value


def extract_job_detail_panel(driver: FirefoxDriver) -> Dict[str, Any]:
    """After clicking a card, read details from the right panel."""
    # Wait until the right panel is present (and updated)
    driver.wait_until_present_by_xpath(
        "//div[contains(@class,'rightcontainer-2NrZP')]"
    )

    root = driver.driver
    right = root.find_element(By.CSS_SELECTOR, "div.rightcontainer-2NrZP")

    details: Dict[str, Any] = {}

    # position location (header line under title)
    try:
        loc_el = right.find_element(By.CSS_SELECTOR, "div.position-location-12ZUO")
        details["detail_position_location"] = _get_text_safe(loc_el)
    except NoSuchElementException:
        details["detail_position_location"] = None

    # Work Mode + Requisition ID (detailContainer-2qNET blocks)
    work_mode = None
    requisition_id = None

    containers = right.find_elements(By.CSS_SELECTOR, "div.detailContainer-2qNET")
    for c in containers:
        try:
            label_el = c.find_element(By.CSS_SELECTOR, "div.detailLabel-2AsIg")
            value_el = c.find_element(By.CSS_SELECTOR, "div.detailValue-3NGwm")
        except NoSuchElementException:
            continue

        label = label_el.text.strip().lower()
        value = value_el.text.strip()

        if label.startswith("work mode"):
            work_mode = value
        elif "requisition id" in label:
            requisition_id = value

    details["detail_work_mode"] = work_mode
    details["detail_requisition_id"] = requisition_id

    # Job description (HTML + text) + salaries (min/max)
    try:
        jd_container = right.find_element(
            By.CSS_SELECTOR, "#job-description-container div.container-3Gm1a"
        )
        desc_html = jd_container.get_attribute("innerHTML")
        desc_text = jd_container.text.strip()
    except NoSuchElementException:
        desc_html = None
        desc_text = None

    details["detail_description_html"] = desc_html
    details["detail_description_text"] = desc_text

    min_salary = None
    max_salary = None
    if desc_text:
        m = re.search(r"Minimum Salary:\s*\$([\d,]+)", desc_text)
        if m:
            min_salary = m.group(1)
        m = re.search(r"Maximum Salary:\s*\$([\d,]+)", desc_text)
        if m:
            max_salary = m.group(1)

    details["detail_min_salary"] = min_salary
    details["detail_max_salary"] = max_salary

    # Insights widget (top skills, previous companies, previous roles)
    insights_top_skills = []
    insights_prev_companies = []
    insights_prev_roles = []

    try:
        insights = right.find_element(
            By.CSS_SELECTOR, "div[data-test-id='insights-widget-container']"
        )
        sub_blocks = insights.find_elements(By.CSS_SELECTOR, "div.subContainer-ifpsE")

        for block in sub_blocks:
            try:
                title_el = block.find_element(By.CSS_SELECTOR, "div.subTitle-3-QLc")
                title = title_el.text.strip().lower()
            except NoSuchElementException:
                continue

            # "Competenze migliori"
            if "competenze migliori" in title:
                spans = block.find_elements(
                    By.CSS_SELECTOR, "span.pills-module_label__yj2be"
                )
                tmp_skills = []
                for s in spans:
                    try:
                        t = s.text.strip()
                    except StaleElementReferenceException:
                        print("[WARN] StaleElement in top skills, skipping one skill")
                        continue
                    if t:
                        tmp_skills.append(t)
                insights_top_skills = tmp_skills

            # "In precedenza ha lavorato presso"
            elif "in precedenza ha lavorato presso" in title:
                imgs = block.find_elements(
                    By.CSS_SELECTOR, "div.logoContainer-QgowM img"
                )
                tmp_companies = []
                for img in imgs:
                    try:
                        alt = (img.get_attribute("alt") or "").strip()
                    except StaleElementReferenceException:
                        print(
                            "[WARN] StaleElement in previous companies, skipping one company"
                        )
                        continue
                    if alt:
                        tmp_companies.append(alt)
                insights_prev_companies = tmp_companies

            # "In precedenza ha lavorato come"
            elif "in precedenza ha lavorato come" in title:
                roles = block.find_elements(By.CSS_SELECTOR, "div.subValue-1IJ6O")
                tmp_roles = []
                for r in roles:
                    try:
                        t = r.text.strip()
                    except StaleElementReferenceException:
                        print(
                            "[WARN] StaleElement in previous roles, skipping one role"
                        )
                        continue
                    if t:
                        tmp_roles.append(t)
                insights_prev_roles = tmp_roles

    except (NoSuchElementException, StaleElementReferenceException) as e:
        print(f"[WARN] Could not fully parse insights widget: {e}")

    details["insights_top_skills"] = insights_top_skills
    details["insights_prev_companies"] = insights_prev_companies
    details["insights_prev_roles"] = insights_prev_roles

    return details



def extract_job_data(driver: FirefoxDriver) -> Dict[str, Any]:
    """
    Use Selenium to read:
      - total jobs
      - pagination info
      - list of job cards in the left sidebar
    """

    # assicurati che la pagina sia caricata: aspetta almeno una card
    driver.wait_until_present_by_xpath("//div[@data-test-id='job-listing']")

    root = driver.driver  # selenium WebDriver vero e proprio

    # -------- total jobs --------
    total_jobs = None
    job_count_el = root.find_element(By.CSS_SELECTOR, "b[data-testid='job-count']")
    m = re.search(r"(\d+)", job_count_el.text)
    if m:
        total_jobs = int(m.group(1))

    # -------- pagination --------
    pagination_info = {
        "current_page": None,
        "total_pages": None,
        "has_previous": None,
        "has_next": None,
    }

    pag_div = root.find_element(
        By.CSS_SELECTOR, "div.pagination-module_pagination__CqWXY"
    )

    # li: <li><span>1</span> <span>of</span> <span>111</span></li>
    spans = pag_div.find_elements(
        By.CSS_SELECTOR, "ul.pagination-module_pager__KjXDQ li span"
    )
    if len(spans) >= 3:
        try:
            pagination_info["current_page"] = int(spans[0].text.strip())
            pagination_info["total_pages"] = int(spans[2].text.strip())
        except ValueError:
            pass

    prev_btn = pag_div.find_element(By.CSS_SELECTOR, "button[aria-label='Previous']")
    next_btn = pag_div.find_element(By.CSS_SELECTOR, "button[aria-label='Next']")

    pagination_info["has_previous"] = (
        prev_btn.get_attribute("aria-disabled") == "false"
    )
    pagination_info["has_next"] = next_btn.get_attribute("aria-disabled") == "false"

    # -------- job cards --------
    jobs: List[Dict[str, Any]] = []

    # only cards in the left list column (avoid 'Similar jobs' carousel)
    left_list = root.find_element(By.CSS_SELECTOR, "div.cardlist-8kM5_")
    cards = left_list.find_elements(
        By.CSS_SELECTOR, "div.cardContainer-GcY1a[data-test-id='job-listing']"
    )

    for idx, card in enumerate(cards):
        # ---- left-side summary (BEFORE click) ----
        link = card.find_element(By.CSS_SELECTOR, "a.r-link.card-F1ebU")
        job_url = link.get_attribute("href")
        card_id_attr = link.get_attribute("id") or ""

        m = re.search(r"job-card-(\d+)-job-list", card_id_attr)
        job_id = m.group(1) if m else None

        title_el = card.find_element(By.CSS_SELECTOR, "div.title-1aNJK")
        title = _get_text_safe(title_el)

        field_values = card.find_elements(
            By.CSS_SELECTOR, "div.fieldsContainer-3Jtts div.fieldValue-3kEar"
        )
        location = field_values[0].text.strip() if len(field_values) > 0 else None
        department = field_values[1].text.strip() if len(field_values) > 1 else None

        tag_spans = card.find_elements(
            By.CSS_SELECTOR,
            "div.pills-module_tag-pills__4H-k6 span.pills-module_label__yj2be",
        )
        tags = [t.text.strip() for t in tag_spans if t.text]

        try:
            published_el = card.find_element(By.CSS_SELECTOR, "div.subData-13Lm1")
            published = _get_text_safe(published_el)
        except NoSuchElementException:
            published = None

        job = {
            "job_id": job_id,
            "job_url": job_url,
            "title": title,
            "location": location,
            "department": department,
            "tags": tags,
            "published": published,
        }

        # ---- click card to load right detail panel ----
        driver.driver.execute_script("arguments[0].click();", link)
        sleep(0.5)

        # ---- right-side detail panel ----
        try:
            detail_data = extract_job_detail_panel(driver)
            job.update(detail_data)
        except Exception as e:
            # Log error but continue with remaining jobs
            print(f"[WARN] Failed to extract detail for job {job_id}: {e}")

        jobs.append(job)

    return {
        "total_jobs": total_jobs,
        "pagination": pagination_info,
        "jobs": jobs,
    }


def _click_next_page(driver: FirefoxDriver) -> bool:
    """Click the Next button in pagination. Return True if we actually moved to next page."""
    root = driver.driver
    try:
        pag_div = root.find_element(
            By.CSS_SELECTOR, "div.pagination-module_pagination__CqWXY"
        )
        next_btn = pag_div.find_element(
            By.CSS_SELECTOR, "button[aria-label='Next']"
        )
    except NoSuchElementException:
        print("[WARN] Next button not found on this page.")
        return False

    if next_btn.get_attribute("aria-disabled") == "true":
        # No next page available
        return False

    # Click next page
    driver.driver.execute_script("arguments[0].click();", next_btn)

    # Wait a bit and try to ensure job list is present
    sleep(3)
    try:
        driver.wait_until_present_by_xpath("//div[@data-test-id='job-listing']")
    except Exception as e:
        print(f"[WARN] After clicking Next, job list did not load correctly: {e}")

    return True


def scrape_all_pages(driver: FirefoxDriver) -> Dict[str, Any]:
    """Iterate over all pages using the Next button and collect all jobs."""
    all_jobs: List[Dict[str, Any]] = []
    total_jobs = None
    last_pagination = None

    while True:
        try:
            page_data = extract_job_data(driver)
        except Exception as e:
            # IMPORTANT: log error but try to move on to next page
            print(f"[ERROR] Failed to extract data on page {current_page + 1}: {e}")

            moved = _click_next_page(driver)
            if not moved:
                print("[ERROR] Cannot move to next page after failure, stopping iteration.")
                break

            current_page += 1
            continue

        # Set total_jobs only once (from the first page header)
        if total_jobs is None:
            total_jobs = page_data["total_jobs"]

        last_pagination = page_data["pagination"]
        current_page = last_pagination["current_page"]
        total_pages = last_pagination["total_pages"]
        has_next = last_pagination["has_next"]

        # Append jobs from this page
        all_jobs.extend(page_data["jobs"])

        print(
            f"Scraped page {current_page}/{total_pages} "
            f"(jobs so far: {len(all_jobs)})"
        )

        # If there is no next page, stop
        if not has_next:
            break

        # try to click Next; if we cannot, stop
        moved = _click_next_page(driver)
        if not moved:
            print("[WARN] Pagination indicates next page, but Next button cannot be used. Stopping.")
            break

    return {
        "total_jobs": total_jobs,
        "pagination": last_pagination,
        "jobs": all_jobs,
    }


def main():
    # metti headless=True se vuoi farlo girare senza finestra
    driver = FirefoxDriver(headless=False)
    driver.init_driver()

    try:
        # carica la pagina (usa Selenium dietro le quinte)
        driver.get_url(START_URL, add_slash=False)
        sleep(5)

        # scrape all pages
        data = scrape_all_pages(driver)

        # __ CSV export __
        jobs = data["jobs"]
        # Collect all possible keys across jobs to build the header
        fieldnames = set()
        for job in jobs:
            fieldnames.update(job.keys())
        fieldnames = sorted(fieldnames)

        output_file = "boston_scientific_jobs.csv"
        with open(output_file, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            for job in jobs:
                row = {k: _to_csv_cell(job.get(k, "")) for k in fieldnames}
                writer.writerow(row)

        print(f"CSV saved to: {output_file}")

        # --- stampa di controllo ---
        print(f"TOTAL JOBS (header): {data['total_jobs']}")
        print(f"TOTAL SCRAPED JOBS: {len(data['jobs'])}")
        p = data["pagination"]
        print(
            f"LAST PAGE: {p['current_page']} / {p['total_pages']} "
            f"(has_prev={p['has_previous']}, has_next={p['has_next']})"
        )
        print()

    finally:
        driver.close_driver()


if __name__ == "__main__":
    main()
