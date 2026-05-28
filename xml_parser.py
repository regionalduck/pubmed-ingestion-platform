from typing import Dict, List, Optional
from lxml import etree
from config import log

def _parse_text(el, xpath: str) -> str:
    nodes = el.xpath(xpath)
    return " ".join("".join(n.itertext()).strip() for n in nodes if n is not None).strip()

def _parse_article(article_el) -> Optional[Dict]:
    try:
        pmid = _parse_text(article_el, ".//PMID")
        if not pmid:
            return None

        title    = _parse_text(article_el, ".//ArticleTitle")
        abstract = _parse_text(article_el, ".//AbstractText")
        journal  = _parse_text(article_el, ".//Journal/Title")

        # ── DOI ──────────────────────────────────────────────────────────────
        doi_nodes = article_el.xpath(".//ArticleId[@IdType='doi']/text()")
        doi = doi_nodes[0].strip() if doi_nodes else ""

        # ── Publication date & year ──────────────────────────────────────────
        pub_date_el = article_el.xpath(".//PubDate")
        pub_date = ""
        pub_year = ""
        if pub_date_el:
            pd = pub_date_el[0]
            year  = _parse_text(pd, "Year")
            month = _parse_text(pd, "Month")
            day   = _parse_text(pd, "Day")
            med   = _parse_text(pd, "MedlineDate")
            if year:
                pub_date = " ".join(filter(None, [year, month, day])).strip()
                pub_year = year
            elif med:
                pub_date = med
                pub_year = med[:4]

        # ── Publication type ─────────────────────────────────────────────────
        pub_type_nodes = article_el.xpath(".//PublicationType/text()")
        pub_type = "; ".join(t.strip() for t in pub_type_nodes if t.strip())

        # ── Authors ──────────────────────────────────────────────────────────
        authors = []
        for position, author_el in enumerate(article_el.xpath(".//Author"), start=1):
            last  = _parse_text(author_el, "LastName")
            first = _parse_text(author_el, "ForeName")
            collective = _parse_text(author_el, "CollectiveName")
            name = f"{last}, {first}".strip(", ") if last else collective
            if not name:
                continue

            affiliation_nodes = author_el.xpath(".//AffiliationInfo/Affiliation/text()")
            affiliation = " | ".join(a.strip() for a in affiliation_nodes if a.strip())

            orcid_nodes = author_el.xpath(".//Identifier[@Source='ORCID']/text()")
            orcid = orcid_nodes[0].strip() if orcid_nodes else ""

            authors.append({
                "name":        name,
                "affiliation": affiliation,
                "orcid":       orcid,
                "position":    position,
            })

        # ── Keywords ─────────────────────────────────────────────────────────
        keywords_json = [kw.strip() for kw in article_el.xpath(".//Keyword/text()") if kw.strip()]

        # ── MeSH terms ───────────────────────────────────────────────────────
        mesh_terms_json = [
            _parse_text(mh, "DescriptorName")
            for mh in article_el.xpath(".//MeshHeading")
            if _parse_text(mh, "DescriptorName")
        ]

        # ── Full-text URL ────────────────────────────────────────────────────
        pmc_nodes = article_el.xpath(".//ArticleId[@IdType='pmc']/text()")
        if pmc_nodes:
            full_text_url = f"https://www.ncbi.nlm.nih.gov/pmc/articles/{pmc_nodes[0].strip()}/"
        else:
            eloc_nodes = article_el.xpath(".//ELocationID[@EIdType='doi']/text()")
            if eloc_nodes:
                full_text_url = f"https://doi.org/{eloc_nodes[0].strip()}"
            else:
                full_text_url = ""

        return {
            "pmid":           pmid,
            "title":          title,
            "abstract":       abstract,
            "doi":            doi,
            "journal":        journal,
            "pub_date":       pub_date,
            "pub_year":       pub_year,
            "pub_type":       pub_type,
            "keywords":       keywords_json,     # Main field used by UI and database search
            "keywords_json":  keywords_json,     # Kept for backward compatibility
            "mesh_terms_json": mesh_terms_json,
            "full_text_url":  full_text_url,
            "authors":        authors,
        }
    except Exception as exc:
        log.warning("Failed to parse article: %s", exc)
        return None

def parse_xml_batch(xml_bytes: bytes, search_term: str, timestamp: str) -> List[Dict]:
    try:
        root = etree.fromstring(xml_bytes)
    except etree.XMLSyntaxError as e:
        log.error("XML parse error: %s", e)
        return []

    docs = []
    for art in root.xpath("//PubmedArticle"):
        doc = _parse_article(art)
        if doc:
            doc["search_term"] = search_term
            doc["ingested_at"] = timestamp
            docs.append(doc)
    return docs
