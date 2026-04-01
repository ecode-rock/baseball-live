# baseball-live

Fetches live pitch-by-pitch data for a single MLB game from Baseball Savant every 5 minutes and commits it to `data/data.csv`. The raw CSV URL can be consumed directly by a frontend app.

---

## How it works

1. GitHub Actions runs `scraper/scraper.py` on a cron every 5 minutes.
2. The scraper calls `https://baseballsavant.mlb.com/gf?game_pk=<GAME_PK>` and writes all pitches to `data/data.csv`, overwriting the file each run (~300 rows for a full game).
3. If the CSV changed, the action commits and pushes it back to `main`.

---

## Setup

### 1. Set `GAME_PK` as a GitHub Actions secret

1. Go to your repo → **Settings** → **Secrets and variables** → **Actions**
2. Click **New repository secret**
3. Name: `GAME_PK`
4. Value: the Baseball Savant `game_pk` integer for the game you want to track (e.g. `777483`)

The scraper reads `GAME_PK` from the environment. You only need to update this secret when you want to switch to a different game.

### 2. Manual trigger with a different game

You can run the workflow on demand for any `game_pk` without changing the secret:

1. Go to **Actions** → **Scrape Live Game** → **Run workflow**
2. Enter a `game_pk` in the input field
3. Click **Run workflow**

The manual `game_pk` input takes priority over the `GAME_PK` secret.

---

## Raw CSV URL

Once the action has run at least once, the data is accessible at:

```
https://raw.githubusercontent.com/<owner>/<repo>/main/data/data.csv
```

Example fetch in JavaScript:

```js
const res = await fetch(
  "https://raw.githubusercontent.com/<owner>/<repo>/main/data/data.csv"
);
const text = await res.text();
```

Use a cache-busting query param (e.g. `?t=` + `Date.now()`) if your app needs to bypass browser caching between polls.

---

## Finding a game_pk

Go to any game page on [Baseball Savant](https://baseballsavant.mlb.com) and look at the URL:

```
https://baseballsavant.mlb.com/gamefeed?game_pk=777483
```

The number after `game_pk=` is the value to use.

---

## Directory structure

```
baseball-live/
├── .github/workflows/scrape.yml   # cron + manual trigger
├── scraper/
│   ├── scraper.py                 # fetch → clean → write CSV
│   └── requirements.txt
├── data/
│   └── data.csv                   # overwritten each run
├── app/
│   └── index.html                 # frontend tracker app
└── README.md
```
