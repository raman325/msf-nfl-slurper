from datetime import date
import json
import logging
import os
import time

from const import TOKEN
import dateutil.parser
from ohmysportsfeedspy import MySportsFeeds
import pytz

START_YEAR = 2014
SLEEP_TIME = 10

msf = MySportsFeeds(version="2.1", store_type=None)

msf.authenticate(TOKEN, "MYSPORTSFEEDS")

BASE_PARAMS = {"format": "json", "league": "nfl", "force": True}
SEASONAL_GAME_FEED = "seasonal_games"
SEASONAL_FEEDS = ["seasonal_team_stats", "seasonal_player_stats"]
WEEKLY_GAME_FEED = "weekly_games"
BY_GAME_FEEDS = ["game_boxscore", "game_playbyplay", "game_lineup"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def get_filename(feed, season, additional_params=None):
    filename = f"season.{season}--feed.{feed}"
    if additional_params:
        for k, v in additional_params.items():
            if k and v:
                filename += f"--{k}.{v}"
            else:
                filename += f"--{k}{v}"
        return f"{filename}.json"
    else:
        return f"{filename}.json"


def get_feeds(feeds, season, additional_params=None):
    errors = []
    params = BASE_PARAMS.copy()
    params["season"] = season
    if additional_params:
        params.update(additional_params)

    for feed in feeds:
        params["feed"] = feed

        json_file = get_filename(feed, season, additional_params)

        if additional_params:
            desc = f"{feed} for {' for '.join(additional_params.values())} for {season}"
        else:
            desc = f"{feed} for {season}"

        if not os.path.isfile(json_file):
            logger.warning(f"Starting download of {desc}")
            logger.warning(f"Params: {params}")
            retry = 1
            while retry > 0:
                try:
                    data = msf.msf_get_data(**params)
                    data.pop("lastUpdatedOn")
                    retry = 0
                except Warning as e:
                    status = int(str(e)[-3:])
                    if status in [429, 499, 502, 503]:
                        logger.warning(
                            f"Attempt {retry} failed, sleeping for {SLEEP_TIME * pow(retry, 2)} seconds"
                        )
                        time.sleep(SLEEP_TIME * pow(retry, 2))
                        retry += 1
                    elif status == 400:
                        logger.warning(f"Malformed request. Skip download of {desc}")

                        retry = -1
                    else:
                        raise e
            if retry == -1 or not data:
                errors.append({**params.copy(), "error": status})
            else:
                with open(json_file, "w") as fp:
                    json.dump(data, fp, indent=2)
            logger.warning(f"Finished downloading {desc}")
        else:
            logger.warning(f"Skipping download of {desc}")

    return errors


def get_game_id(game):
    game_date_raw = dateutil.parser.isoparse(game["schedule"]["startTime"]).astimezone(
        pytz.timezone("US/Eastern")
    )
    game_date = game_date_raw.strftime("%Y%m%d")
    game_away_team = game["schedule"]["awayTeam"]["abbreviation"]
    game_home_team = game["schedule"]["homeTeam"]["abbreviation"]
    return f"{game_date}-{game_away_team}-{game_home_team}"


def get_full_season_data(start_year=START_YEAR):
    now = date.today()
    next_full_season_year = now.year if now.month < 9 else now.year - 1

    for year in range(start_year, next_full_season_year):
        season = f"{year}-{year + 1}-regular"
        logger.warning(f"Downloading data for {season} season")
        if not get_feeds(SEASONAL_FEEDS + [SEASONAL_GAME_FEED], season):
            for feed in BY_GAME_FEEDS:
                feed_file = get_filename(feed, season)
                if not os.path.isfile(feed_file):
                    logger.warning(f"Downloading {feed} game feeds for {season} season")
                    with open(get_filename(SEASONAL_GAME_FEED, season), "r") as fp:
                        games = json.load(fp)["games"]
                    errors = []
                    for game in games:
                        errors += get_feeds([feed], season, {"game": get_game_id(game)})

                    if not errors:
                        logger.warning(
                            f"Retrieved all {feed} game feeds for {season} season"
                        )
                        logger.warning(f"Generating {feed_file}")
                        data_list = []
                        for game in games:
                            game_file = get_filename(
                                feed, season, {"game": get_game_id(game)}
                            )
                            with open(game_file, "r") as fp:
                                game_dict = json.load(fp)

                            data_list.append(game_dict)

                        if os.path.isfile(feed_file):
                            os.remove(feed_file)

                        with open(feed_file, "w") as fp:
                            json.dump(data_list, fp, indent=2)
                        logger.warning(f"Done generating {feed_file}")

                        logger.warning(
                            f"Deleting all {feed} game feeds for {season} season"
                        )
                        for game in games:
                            game_file = get_filename(
                                feed, season, {"game": get_game_id(game)}
                            )
                            if os.path.isfile(game_file):
                                os.remove(game_file)
                        logger.warning(
                            f"Done deleting all {feed} game feeds for {season} season"
                        )
                    else:
                        error_file = get_filename(feed, season, {"errors": ""})
                        if os.path.isfile(error_file):
                            os.remove(error_file)
                        with open(error_file, "w") as fp:
                            json.dump(errors, fp, indent=2)
                        logger.warning(
                            f"Could not download all {feed} game feeds for {season} season, dumped error list to {error_file}"
                        )
                else:
                    logger.warning(
                        f"Skipping download of {feed} game feeds for {season} season since {feed_file} exists"
                    )
        else:
            logger.warning(f"Could not download all seasonal data for {season} season")


def get_data_for_week(week, year_season_starts=None):
    if not year_season_starts:
        now = date.today()
        year_season_starts = now.year if now.month > 8 else now.year - 1

    season = f"{year_season_starts}-{year_season_starts + 1}-regular"
    logger.warning(f"Downloading data for week {week} of {season} season")
    if not get_feeds([WEEKLY_GAME_FEED], season, {"week": week}):
        for feed in BY_GAME_FEEDS:
            feed_file = get_filename(feed, season, {"week": week})
            if not os.path.isfile(feed_file):
                logger.warning(
                    f"Downloading {feed} game feeds for week {week} of {season} season"
                )
                with open(
                    get_filename(SEASONAL_GAME_FEED, season, {"week": week}), "r"
                ) as fp:
                    games = json.load(fp)["games"]
                errors = []
                for game in games:
                    errors += get_feeds([feed], season, {"game": get_game_id(game)})

                if not errors:
                    logger.warning(
                        f"Retrieved all {feed} game feeds for week {week} of {season} season"
                    )
                    logger.warning(f"Generating {feed_file}")
                    data_list = []
                    for game in games:
                        game_file = get_filename(
                            feed, season, {"game": get_game_id(game)}
                        )
                        with open(game_file, "r") as fp:
                            game_dict = json.load(fp)

                        data_list.append(game_dict)

                    if os.path.isfile(feed_file):
                        os.remove(feed_file)

                    with open(feed_file, "w") as fp:
                        json.dump(data_list, fp, indent=2)
                    logger.warning(f"Done generating {feed_file}")
                else:
                    error_file = get_filename(
                        feed, season, {"week": week, "errors": ""}
                    )
                    if os.path.isfile(error_file):
                        os.remove(error_file)
                    with open(error_file, "w") as fp:
                        json.dump(errors, fp, indent=2)
                    logger.warning(
                        f"Could not download all {feed} game feeds for week {week} of {season} season, dumped error list to {error_file}"
                    )
            else:
                logger.warning(
                    f"Skipping download of {feed} game feeds for week {week} of {season} season since {feed_file} exists"
                )
    else:
        logger.warning(f"Could not download all data for week {week} {season} season")


get_full_season_data()
