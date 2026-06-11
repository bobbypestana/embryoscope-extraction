import os
import re
import logging
from datetime import datetime

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger("validation_hooks")

def get_columns(conn, table_name):
    """
    Database-agnostic way to retrieve column names from a table.
    """
    cursor = conn.execute(f"SELECT * FROM {table_name} LIMIT 0")
    return [desc[0] for desc in cursor.description]

def pre_run_schema_safeguard(conn, table_a, table_b, join_keys, conn_b=None, join_keys_b=None):
    """
    Hook 1: Pre-Run Schema Safeguard
    Verifies that tables exist and that all specified join_keys are present.
    """
    logger.info(f"Running pre-run schema safeguard for tables: {table_a} and {table_b}")
    if conn_b is None:
        conn_b = conn
    if join_keys_b is None:
        join_keys_b = join_keys
    
    # 1. Check table existence
    try:
        conn.execute(f"SELECT 1 FROM {table_a} LIMIT 1")
    except Exception as e:
        msg = f"Table '{table_a}' does not exist or is not queryable: {str(e)}"
        logger.error(msg)
        raise ValueError(msg)
        
    try:
        conn_b.execute(f"SELECT 1 FROM {table_b} LIMIT 1")
    except Exception as e:
        msg = f"Table '{table_b}' does not exist or is not queryable: {str(e)}"
        logger.error(msg)
        raise ValueError(msg)
            
    # 2. Retrieve columns and verify join keys
    try:
        cols_a = [c.lower() for c in get_columns(conn, table_a)]
        cols_b = [c.lower() for c in get_columns(conn_b, table_b)]
    except Exception as e:
        msg = f"Failed to retrieve columns from tables: {str(e)}"
        logger.error(msg)
        raise ValueError(msg)
        
    for k in join_keys:
        k_low = k.lower()
        if k_low not in cols_a:
            msg = f"Join key '{k}' not found in table '{table_a}' columns: {cols_a}"
            logger.error(msg)
            raise ValueError(msg)
            
    for k in join_keys_b:
        k_low = k.lower()
        if k_low not in cols_b:
            msg = f"Join key '{k}' not found in table '{table_b}' columns: {cols_b}"
            logger.error(msg)
            raise ValueError(msg)
            
    logger.info("Pre-run schema safeguard passed successfully.")
    return True

def scrub_pii(text_content):
    """
    Hook 2: Automatic PII-Scrubbing/Anonymization
    Redacts emails, phone numbers, CPFs, and CNPJs to protect personal data.
    """
    if not isinstance(text_content, str):
        return text_content
        
    # Email patterns
    email_pattern = re.compile(r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}')
    text_content = email_pattern.sub("[EMAIL_REDACTED]", text_content)
    
    # Phone number patterns (common Brazilian and international formats)
    phone_pattern = re.compile(r'\+?\d{1,3}?[-.\s]?\(?\d{2,3}\)?[-.\s]?\d{4,5}[-.\s]?\d{4}')
    text_content = phone_pattern.sub("[PHONE_REDACTED]", text_content)
    
    # Brazilian CPF (formatted: 000.000.000-00)
    cpf_pattern = re.compile(r'\b\d{3}\.\d{3}\.\d{3}-\d{2}\b')
    text_content = cpf_pattern.sub("[CPF_REDACTED]", text_content)
    
    # Brazilian CNPJ (formatted: 00.000.000/0000-00)
    cnpj_pattern = re.compile(r'\b\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}\b')
    text_content = cnpj_pattern.sub("[CNPJ_REDACTED]", text_content)
    
    return text_content

def archive_report(markdown_content, comparison_type, base_dir="data_lake_validation/published_reports"):
    """
    Hook 3: Report Archival & Publication
    Scrubs PII and saves the report as a timestamped Markdown file.
    """
    os.makedirs(base_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Sanitize comparison name for file system compatibility
    comp_type_clean = "".join(c if (c.isalnum() or c in ("_", "-")) else "_" for c in comparison_type).lower()
    filename = f"{timestamp}_{comp_type_clean}.md"
    filepath = os.path.join(base_dir, filename)
    
    # Scrub PII before saving
    sanitized_content = scrub_pii(markdown_content)
    
    with open(filepath, "w", encoding="utf-8") as f:
        f.write(sanitized_content)
        
    logger.info(f"Report successfully archived to: {filepath}")
    return filepath

def evaluate_thresholds(metrics, thresholds=None):
    """
    Hook 4: Data Quality Threshold Alert
    Checks a dictionary of metrics against minimum limits.
    """
    if thresholds is None:
        thresholds = {
            'client_match_rate': 0.95,
            'patient_match_rate': 0.95,
            'financial_alignment_rate': 0.9999,
            'row_count_pct_diff': 0.02
        }
        
    failures = []
    
    for key, limit in thresholds.items():
        if key not in metrics:
            continue
            
        val = metrics[key]
        if key == 'row_count_pct_diff':
            # Row count difference percent (e.g. 0.005 is 0.5%)
            if abs(val) > limit:
                failures.append(f"Row count difference too high: {val*100:.3f}% (Limit: +/- {limit*100:.3f}%)")
        else:
            # Match/Alignment rates (should be above the limit)
            if val < limit:
                failures.append(f"Metric '{key}' fell below threshold: {val*100:.3f}% (Limit: {limit*100:.3f}%)")
                
    passed = len(failures) == 0
    logger.info(f"Threshold check results: Passed={passed}, Failures={len(failures)}")
    return {
        'passed': passed,
        'failures': failures
    }

def format_jira_comment(metrics, threshold_results, report_filepath):
    """
    Hook 5: Jira Kanban Update Hook (Comment Formatter)
    Formats a Jira-compatible comment. Also attempts to post via REST API
    if Jira credentials are found in the environment, otherwise returns the text for MCP posting.
    """
    status = "✅ PASS" if threshold_results['passed'] else "❌ FAIL"
    report_basename = os.path.basename(report_filepath)
    
    report_abs_path = os.path.abspath(report_filepath).replace('\\', '/')
    comment = f"h3. Data Lake Table Reconciliation: {status}\n"
    comment += f"*Artifact:* [{report_basename}|file:///{report_abs_path}]\n\n"
    
    comment += "||Metric Name||Reconciliation Value||Status||\n"
    for k, v in metrics.items():
        label = k.replace('_', ' ').title()
        val_str = f"{v*100:.3f}%" if ('rate' in k or 'pct' in k) else f"{v}"
        m_status = "✅ OK"
        # Check if metric was in the failure messages
        for f in threshold_results['failures']:
            if k in f.lower() or label.lower() in f.lower():
                m_status = "❌ ALERT"
        comment += f"| {label} | {val_str} | {m_status} |\n"
        
    if not threshold_results['passed']:
        comment += "\n*Quality Gate Alerts:*\n"
        for f in threshold_results['failures']:
            comment += f"* {f}\n"
            
    return comment

def post_jira_comment(ticket_id, metrics, threshold_results, report_filepath):
    """
    Attempts to post comment directly to Jira if config exists, otherwise logs the output.
    """
    comment_text = format_jira_comment(metrics, threshold_results, report_filepath)
    
    jira_url = os.environ.get("JIRA_API_URL")
    jira_user = os.environ.get("JIRA_USER")
    jira_token = os.environ.get("JIRA_TOKEN")
    
    if jira_url and jira_user and jira_token:
        try:
            import requests
            from requests.auth import HTTPBasicAuth
            
            url = f"{jira_url.rstrip('/')}/rest/api/2/issue/{ticket_id}/comment"
            headers = {"Accept": "application/json", "Content-Type": "application/json"}
            auth = HTTPBasicAuth(jira_user, jira_token)
            payload = {"body": comment_text}
            
            response = requests.post(url, json=payload, headers=headers, auth=auth, timeout=10)
            if response.status_code == 201:
                logger.info(f"Successfully posted comment to Jira ticket {ticket_id}")
                return True
            else:
                logger.error(f"Failed to post to Jira ({response.status_code}): {response.text}")
        except Exception as e:
            logger.error(f"Error posting comment to Jira API: {str(e)}")
            
    logger.info(f"Jira REST environment variables not found or API call failed. Below is the formatted Jira comment for manual or MCP posting:\n\n{comment_text}\n")
    return comment_text
