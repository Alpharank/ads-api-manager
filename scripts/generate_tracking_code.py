#!/usr/bin/env python3
"""
Generate Google Ads tracking code snippets for a client website.

Reads conversion action IDs/labels from audit.json and generates
ready-to-install HTML/JS snippets for snowbirdship.com.

Usage:
    python scripts/generate_tracking_code.py --client embenauto
"""

import argparse
import json
import os
import re
import sys

from gads_helpers import load_config, get_customer_id, client_data_dir


def load_audit(client_slug):
    """Load conversion actions from audit.json."""
    audit_path = os.path.join(client_data_dir(client_slug), "conversion_actions", "audit.json")
    if not os.path.exists(audit_path):
        print(f"  No audit.json found. Run audit_conversion_actions.py first.")
        sys.exit(1)
    with open(audit_path) as f:
        return json.load(f)


def extract_conversion_id(actions):
    """Extract the Google Ads conversion ID (AW-XXXXXXXXX) from tag snippets."""
    for action in actions:
        for snippet in action.get("tag_snippets", []):
            global_tag = snippet.get("global_site_tag", "")
            match = re.search(r"AW-(\d+)", global_tag)
            if match:
                return match.group(1)
    return None


def extract_conversion_label(action):
    """Extract conversion label from an action's event snippet."""
    for snippet in action.get("tag_snippets", []):
        event_snip = snippet.get("event_snippet", "")
        match = re.search(r"send_to.*?AW-\d+/([A-Za-z0-9_-]+)", event_snip)
        if match:
            return match.group(1)
    return None


def find_action_by_name(actions, name_pattern):
    """Find a conversion action by partial name match (case-insensitive)."""
    pattern = name_pattern.lower()
    for a in actions:
        if pattern in a["name"].lower():
            return a
    return None


def generate_global_tag(conversion_id, out_dir):
    """Generate the global site tag (gtag.js) snippet."""
    html = f"""<!-- Google Ads Global Site Tag (gtag.js) -->
<!-- Place this in the <head> of EVERY page on snowbirdship.com -->
<script async src="https://www.googletagmanager.com/gtag/js?id=AW-{conversion_id}"></script>
<script>
  window.dataLayer = window.dataLayer || [];
  function gtag(){{ dataLayer.push(arguments); }}
  gtag('js', new Date());
  gtag('config', 'AW-{conversion_id}');
</script>
"""
    path = os.path.join(out_dir, "gtag_global.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  Written: {path}")
    return path


def generate_form_submit_tag(conversion_id, label, out_dir):
    """Generate the form submission event snippet."""
    if not label:
        label = "FORM_SUBMIT_LABEL"
        print("  WARNING: No form submission conversion label found. Using placeholder.")

    html = f"""<!-- Google Ads Form Submission Conversion Tracking -->
<!-- Fire this when the quote form is successfully submitted -->
<!-- Option 1: Inline in form onsubmit handler -->
<script>
  function trackFormSubmission() {{
    gtag('event', 'conversion', {{
      'send_to': 'AW-{conversion_id}/{label}',
      'value': 50.0,
      'currency': 'USD'
    }});
  }}
</script>

<!-- Option 2: Event listener (add to your main JS file) -->
<script>
  document.addEventListener('DOMContentLoaded', function() {{
    // Adjust selector to match your quote form
    var form = document.querySelector('form[action*="quote"], form.quote-form, #quoteForm');
    if (form) {{
      form.addEventListener('submit', function() {{
        gtag('event', 'conversion', {{
          'send_to': 'AW-{conversion_id}/{label}',
          'value': 50.0,
          'currency': 'USD'
        }});
      }});
    }}
  }});
</script>
"""
    path = os.path.join(out_dir, "gtag_form_submit.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  Written: {path}")
    return path


def generate_thankyou_page_tag(conversion_id, label, out_dir):
    """Generate the thank you page conversion snippet."""
    if not label:
        label = "THANKYOU_PAGE_LABEL"
        print("  WARNING: No thank you page conversion label found. Using placeholder.")

    html = f"""<!-- Google Ads Thank You Page Conversion Tracking -->
<!-- Place this ONLY on your thank-you / confirmation page -->
<!-- (e.g. snowbirdship.com/thank-you or snowbirdship.com/quote-received) -->
<script>
  gtag('event', 'conversion', {{
    'send_to': 'AW-{conversion_id}/{label}',
    'value': 10.0,
    'currency': 'USD'
  }});
</script>
"""
    path = os.path.join(out_dir, "gtag_thankyou_page.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  Written: {path}")
    return path


def generate_phone_call_tag(conversion_id, label, out_dir):
    """Generate the website phone call tracking snippet."""
    if not label:
        label = "PHONE_CALL_LABEL"
        print("  WARNING: No phone call conversion label found. Using placeholder.")

    html = f"""<!-- Google Ads Website Phone Call Tracking -->
<!-- Place this on pages with your phone number -->
<!-- This tracks clicks on tel: links as conversions -->
<script>
  document.addEventListener('DOMContentLoaded', function() {{
    // Track clicks on phone number links
    var phoneLinks = document.querySelectorAll('a[href^="tel:"]');
    phoneLinks.forEach(function(link) {{
      link.addEventListener('click', function() {{
        gtag('event', 'conversion', {{
          'send_to': 'AW-{conversion_id}/{label}',
          'value': 25.0,
          'currency': 'USD'
        }});
      }});
    }});
  }});
</script>
"""
    path = os.path.join(out_dir, "gtag_phone_call.html")
    with open(path, "w") as f:
        f.write(html)
    print(f"  Written: {path}")
    return path


def generate_gclid_capture(out_dir):
    """Generate GCLID capture script (stores in cookie + populates hidden fields)."""
    js = """/**
 * GCLID Capture — stores Google Click ID from URL into a cookie
 * and populates hidden form fields for offline conversion tracking.
 *
 * Add to every page. Captures ?gclid= param on landing, persists for 90 days.
 */
(function() {
  'use strict';

  var COOKIE_NAME = '_gclid';
  var COOKIE_DAYS = 90;

  function getParam(name) {
    var match = RegExp('[?&]' + name + '=([^&]*)').exec(window.location.search);
    return match && decodeURIComponent(match[1].replace(/\\+/g, ' '));
  }

  function setCookie(name, value, days) {
    var expires = '';
    if (days) {
      var date = new Date();
      date.setTime(date.getTime() + (days * 24 * 60 * 60 * 1000));
      expires = '; expires=' + date.toUTCString();
    }
    document.cookie = name + '=' + (value || '') + expires + '; path=/; SameSite=Lax';
  }

  function getCookie(name) {
    var nameEQ = name + '=';
    var ca = document.cookie.split(';');
    for (var i = 0; i < ca.length; i++) {
      var c = ca[i].trim();
      if (c.indexOf(nameEQ) === 0) return c.substring(nameEQ.length);
    }
    return null;
  }

  // Capture GCLID from URL on landing
  var gclid = getParam('gclid');
  if (gclid) {
    setCookie(COOKIE_NAME, gclid, COOKIE_DAYS);
  }

  // Populate hidden form fields with stored GCLID
  function populateGclidFields() {
    var storedGclid = getCookie(COOKIE_NAME);
    if (!storedGclid) return;

    var fields = document.querySelectorAll('input[name="gclid"], input[name="google_click_id"]');
    fields.forEach(function(field) {
      field.value = storedGclid;
    });
  }

  // Run on DOM ready and after any dynamic form loads
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', populateGclidFields);
  } else {
    populateGclidFields();
  }

  // Re-populate on any form that appears later (mutation observer)
  if (typeof MutationObserver !== 'undefined') {
    var observer = new MutationObserver(function(mutations) {
      for (var i = 0; i < mutations.length; i++) {
        if (mutations[i].addedNodes.length) {
          populateGclidFields();
          break;
        }
      }
    });
    observer.observe(document.body || document.documentElement, {
      childList: true,
      subtree: true
    });
  }
})();
"""
    path = os.path.join(out_dir, "gclid_capture.js")
    with open(path, "w") as f:
        f.write(js)
    print(f"  Written: {path}")
    return path


def generate_readme(conversion_id, out_dir):
    """Generate installation instructions README."""
    md = f"""# Google Ads Tracking Code — snowbirdship.com

## Conversion ID: AW-{conversion_id}

## Installation Order

### 1. Global Site Tag (`gtag_global.html`)
Add to the `<head>` of **every page** on snowbirdship.com.
This loads the gtag.js library and configures your Google Ads account.

### 2. GCLID Capture (`gclid_capture.js`)
Add to **every page**, after the global site tag.
This captures the `?gclid=` parameter from Google Ads clicks and stores it
in a cookie for 90 days. It also auto-populates any hidden `<input name="gclid">` fields.

**Important:** Add a hidden field to your quote form:
```html
<input type="hidden" name="gclid" value="">
```

### 3. Form Submission Tracking (`gtag_form_submit.html`)
Add to pages with your quote request form.
Fires a conversion event when the form is submitted.

### 4. Thank You Page Tracking (`gtag_thankyou_page.html`)
Add **only** to your thank-you / confirmation page.
Fires a backup conversion for quote completions.

### 5. Phone Call Tracking (`gtag_phone_call.html`)
Add to pages that display your phone number.
Tracks clicks on `tel:` links as call conversions.

## Testing

1. Use [Google Tag Assistant](https://tagassistant.google.com/) to verify tags fire
2. Submit a test quote — check Google Ads > Conversions for the event
3. Click your phone number link — verify call conversion fires
4. Visit with `?gclid=test123` — verify cookie is set (check Application > Cookies in DevTools)

## Notes

- Conversion labels (e.g., `AW-{conversion_id}/xxxxx`) are action-specific.
  If labels show as placeholders, re-run `audit_conversion_actions.py` after creating
  the actions to get the real labels, then re-run this script.
- Phone Call from Ad (call extensions) is configured in Google Ads UI, not on your website.
"""
    path = os.path.join(out_dir, "README.md")
    with open(path, "w") as f:
        f.write(md)
    print(f"  Written: {path}")
    return path


def main():
    parser = argparse.ArgumentParser(description="Generate tracking code snippets")
    parser.add_argument("--client", required=True, help="Client slug (e.g. embenauto)")
    parser.add_argument("--config", default=None, help="Path to config.yaml")
    args = parser.parse_args()

    config = load_config(args.config)
    customer_id = get_customer_id(config, args.client)
    actions = load_audit(args.client)

    conversion_id = extract_conversion_id(actions)
    if not conversion_id:
        # Fall back to customer ID as the conversion ID
        conversion_id = customer_id
        print(f"  No AW-ID found in tag snippets, using customer ID: AW-{conversion_id}")

    out_dir = os.path.join(client_data_dir(args.client), "tracking")
    os.makedirs(out_dir, exist_ok=True)

    print(f"Generating tracking code for AW-{conversion_id}...\n")

    # Find specific actions and their labels
    form_action = find_action_by_name(actions, "form submission") or find_action_by_name(actions, "quote")
    thankyou_action = find_action_by_name(actions, "thank you") or find_action_by_name(actions, "page view")
    phone_action = find_action_by_name(actions, "phone call from website") or find_action_by_name(actions, "phone call")

    form_label = extract_conversion_label(form_action) if form_action else None
    thankyou_label = extract_conversion_label(thankyou_action) if thankyou_action else None
    phone_label = extract_conversion_label(phone_action) if phone_action else None

    generate_global_tag(conversion_id, out_dir)
    generate_form_submit_tag(conversion_id, form_label, out_dir)
    generate_thankyou_page_tag(conversion_id, thankyou_label, out_dir)
    generate_phone_call_tag(conversion_id, phone_label, out_dir)
    generate_gclid_capture(out_dir)
    generate_readme(conversion_id, out_dir)

    print(f"\n  All snippets written to: {out_dir}/")
    print(f"  See README.md for installation instructions.")


if __name__ == "__main__":
    main()
