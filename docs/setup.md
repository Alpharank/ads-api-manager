# Setup

## 1. Install Dependencies

```bash
pip install -r requirements.txt
```

## 2. Configure Credentials

```bash
cp config/config.example.yaml config/config.yaml
```

Edit with Google Ads and AWS credentials. See [`config/config.example.yaml`](../config/config.example.yaml).

## 3. Generate OAuth Token (if needed)

```bash
python scripts/generate_refresh_token.py
```
