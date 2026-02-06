#!/usr/bin/env python3
"""
Generate daily keyword-level attribution data for all three models.

Creates enriched/daily/{month}.csv with columns:
date, campaign_id, campaign_name, ad_group_id, ad_group_name, keyword, match_type,
apps, approved, funded, production, value

Also creates _first_click.csv and _linear.csv variants.
"""

import os
import csv
import random
from collections import defaultdict
from datetime import datetime

DATA_DIR = os.path.join(os.path.dirname(__file__), '..', 'data', 'kitsap_cu')

def load_daily_data(month):
    """Load daily campaign data."""
    path = os.path.join(DATA_DIR, 'daily', f'{month}.csv')
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return list(csv.DictReader(f))

def load_keyword_data(month):
    """Load keyword data."""
    path = os.path.join(DATA_DIR, 'keywords', f'{month}.csv')
    if not os.path.exists(path):
        return []
    with open(path, 'r') as f:
        return list(csv.DictReader(f))

def load_enriched_data(month, model=''):
    """Load monthly enriched campaign data for an attribution model."""
    suffix = f'_{model}' if model else ''
    path = os.path.join(DATA_DIR, 'enriched', f'{month}{suffix}.csv')
    if not os.path.exists(path):
        return {}
    with open(path, 'r') as f:
        return {r['campaign_id']: r for r in csv.DictReader(f)}

def generate_daily_keyword_attribution(month, model=''):
    """
    Generate daily keyword-level attribution data.

    Strategy:
    1. Load monthly enriched campaign data (totals for apps, funded, value)
    2. Load daily campaign data (cost by date by campaign)
    3. Load keyword data (cost share within campaign)
    4. Distribute campaign totals to days proportionally by daily cost
    5. Distribute daily totals to keywords proportionally by keyword cost share
    """
    daily_data = load_daily_data(month)
    keyword_data = load_keyword_data(month)
    enriched = load_enriched_data(month, model)

    if not daily_data or not enriched:
        print(f"  Skipping {month} - missing data")
        return []

    # Compute campaign total costs from daily data
    campaign_daily_cost = defaultdict(lambda: defaultdict(float))  # {campaign_id: {date: cost}}
    campaign_total_cost = defaultdict(float)

    for row in daily_data:
        cid = row['campaign_id']
        date = row['date']
        cost = float(row['cost'] or 0)
        campaign_daily_cost[cid][date] += cost
        campaign_total_cost[cid] += cost

    # Compute keyword cost share within campaigns
    campaign_keyword_cost = defaultdict(lambda: defaultdict(float))  # {campaign_id: {(ag_id, kw): cost}}

    for row in keyword_data:
        cid = row['campaign_id']
        key = (row['ad_group_id'], row['ad_group_name'], row['keyword'], row['match_type'])
        cost = float(row['cost'] or 0)
        campaign_keyword_cost[cid][key] += cost

    # Generate daily keyword rows
    output_rows = []

    for cid, dates_cost in campaign_daily_cost.items():
        if cid not in enriched:
            continue

        enr = enriched[cid]
        total_apps = float(enr.get('apps', 0) or 0)
        total_approved = float(enr.get('approved', 0) or 0)
        total_funded = float(enr.get('funded', 0) or 0)
        total_production = float(enr.get('production', 0) or 0)
        total_value = float(enr.get('value', 0) or 0)

        if total_apps == 0 and total_funded == 0:
            continue

        camp_total_cost = campaign_total_cost[cid]
        keywords = campaign_keyword_cost.get(cid, {})

        if not keywords:
            # No keyword data - create a placeholder row per day
            for date, day_cost in dates_cost.items():
                if camp_total_cost <= 0:
                    continue
                day_share = day_cost / camp_total_cost

                output_rows.append({
                    'date': date,
                    'campaign_id': cid,
                    'campaign_name': daily_data[0].get('campaign_name', ''),  # Get from first row
                    'ad_group_id': '',
                    'ad_group_name': '(no keyword data)',
                    'keyword': '(no keyword data)',
                    'match_type': '',
                    'apps': round(total_apps * day_share, 4),
                    'approved': round(total_approved * day_share, 4),
                    'funded': round(total_funded * day_share, 4),
                    'production': round(total_production * day_share, 4),
                    'value': round(total_value * day_share, 4)
                })
            continue

        # Calculate keyword cost shares within campaign
        kw_total_cost = sum(keywords.values())

        for date, day_cost in dates_cost.items():
            if camp_total_cost <= 0:
                continue
            day_share = day_cost / camp_total_cost

            # Daily campaign totals
            day_apps = total_apps * day_share
            day_approved = total_approved * day_share
            day_funded = total_funded * day_share
            day_production = total_production * day_share
            day_value = total_value * day_share

            # Distribute to keywords by cost share
            for (ag_id, ag_name, kw, match_type), kw_cost in keywords.items():
                if kw_total_cost <= 0:
                    kw_share = 1 / len(keywords)
                else:
                    kw_share = kw_cost / kw_total_cost

                # Get campaign name from daily data
                camp_name = ''
                for row in daily_data:
                    if row['campaign_id'] == cid:
                        camp_name = row['campaign_name']
                        break

                output_rows.append({
                    'date': date,
                    'campaign_id': cid,
                    'campaign_name': camp_name,
                    'ad_group_id': ag_id,
                    'ad_group_name': ag_name,
                    'keyword': kw,
                    'match_type': match_type,
                    'apps': round(day_apps * kw_share, 4),
                    'approved': round(day_approved * kw_share, 4),
                    'funded': round(day_funded * kw_share, 4),
                    'production': round(day_production * kw_share, 4),
                    'value': round(day_value * kw_share, 4)
                })

    return sorted(output_rows, key=lambda x: (x['date'], x['campaign_id'], x['keyword']))


def write_daily_attribution(month, rows, model=''):
    """Write daily attribution data to CSV."""
    os.makedirs(os.path.join(DATA_DIR, 'enriched', 'daily'), exist_ok=True)

    suffix = f'_{model}' if model else ''
    path = os.path.join(DATA_DIR, 'enriched', 'daily', f'{month}{suffix}.csv')

    fieldnames = ['date', 'campaign_id', 'campaign_name', 'ad_group_id', 'ad_group_name',
                  'keyword', 'match_type', 'apps', 'approved', 'funded', 'production', 'value']

    with open(path, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"  Wrote {len(rows)} rows to {path}")


def main():
    # Process available months
    months = []
    enriched_dir = os.path.join(DATA_DIR, 'enriched')
    for f in os.listdir(enriched_dir):
        if f.endswith('.csv') and not f.startswith('.') and '_' not in f:
            months.append(f.replace('.csv', ''))

    months.sort(reverse=True)

    print(f"Processing months: {months}")

    for month in months:
        print(f"\n{month}:")

        # Generate for each attribution model
        for model in ['', 'first_click', 'linear']:
            model_name = model if model else 'last_click (default)'
            print(f"  Generating {model_name}...")

            rows = generate_daily_keyword_attribution(month, model)
            if rows:
                write_daily_attribution(month, rows, model)


if __name__ == '__main__':
    main()
