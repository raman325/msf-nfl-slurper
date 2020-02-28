from datetime import datetime, timedelta
import json
import logging
import os
import time
from typing import Any, Dict, List, Tuple

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


def get_filename(
    feed: str, season: str, additional_params: Dict[str, str] = None
) -> str:
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


def get_feeds(
    feeds: List[str],
    season: str,
    additional_params: Dict[str, str] = None,
    additional_params_to_try: List[Dict[str, str]] = None,
) -> List[Dict[str, str]]:
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
                        if additional_params_to_try:
                            for try_params in additional_params_to_try:
                                logger.warning(
                                    f"Malformed request. Trying download of {desc} using alternate params {try_params}"
                                )
                                if not get_feeds([feed], season, try_params):
                                    retry = 0
                                    break
                                else:
                                    retry = -1
                        else:
                            logger.warning(
                                f"Malformed request. Skip download of {desc}"
                            )
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


def get_game_ids(game: Dict[str, Any]) -> Tuple[str, str, str]:
    game_date_raw = dateutil.parser.isoparse(game["schedule"]["startTime"]).astimezone(
        pytz.timezone("US/Eastern")
    )
    game_date = game_date_raw.strftime("%Y%m%d")
    plusone_game_date = (game_date_raw + timedelta(days=1)).strftime("%Y%m%d")
    minusone_game_date = (game_date_raw - timedelta(days=1)).strftime("%Y%m%d")
    game_away_team = game["schedule"]["awayTeam"]["abbreviation"]
    game_home_team = game["schedule"]["homeTeam"]["abbreviation"]
    return (
        f"{game_date}-{game_away_team}-{game_home_team}",
        f"{plusone_game_date}-{game_away_team}-{game_home_team}",
        f"{minusone_game_date}-{game_away_team}-{game_home_team}",
    )


def get_game_file(feed: str, season: str, game: Dict[str, Any]) -> str:
    (game_id, plusone_game_id, minusone_game_id) = get_game_ids(game)
    game_file = get_filename(feed, season, {"game": game_id})
    plusone_game_file = get_filename(feed, season, {"game": plusone_game_id})
    minusone_game_file = get_filename(feed, season, {"game": minusone_game_id})
    if os.path.isfile(game_file):
        return game_file
    elif os.path.isfile(plusone_game_file):
        return plusone_game_file
    else:
        return minusone_game_file


def delete_weekly_feeds_for_season(season: str) -> None:
    week_feeds_to_delete = []
    for filename in os.listdir("."):
        if "--week." in filename and f"season.{season}" in filename:
            week_feeds_to_delete += filename

    if week_feeds_to_delete:
        logger.warning(f"Deleting all weekly feeds for {season} season")
        for filename in week_feeds_to_delete:
            os.remove(filename)
        logger.warning(f"Done deleting all weekly feeds for {season} season")


def delete_games_for_season_and_feed(season: str, feed: str) -> None:
    game_feeds_to_delete = []
    for filename in os.listdir("."):
        if (
            "--game." in filename
            and f"season.{season}" in filename
            and f"feed.{feed}" in filename
        ):
            game_feeds_to_delete += filename

    if game_feeds_to_delete:
        logger.warning(f"Deleting all {feed} game feeds for {season} season")
        for filename in game_feeds_to_delete:
            os.remove(filename)
        logger.warning(f"Done deleting all {feed} game feeds for {season} season")


def get_full_season_data(start_year: int = START_YEAR) -> None:
    now = datetime.today()
    next_full_season_start_year = now.year if now.month > 2 else now.year - 1

    for year in range(start_year, next_full_season_start_year):
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
                        (game_id, plusone_game_id, minusone_game_id) = get_game_ids(
                            game
                        )
                        errors += get_feeds(
                            [feed],
                            season,
                            {"game": game_id},
                            [{"game": plusone_game_id}, {"game": minusone_game_id}],
                        )

                    if not errors:
                        logger.warning(
                            f"Retrieved all {feed} game feeds for {season} season"
                        )
                        logger.warning(f"Generating {feed_file}")
                        data_list = []
                        for game in games:
                            with open(get_game_file(feed, season, game), "r") as fp:
                                game_dict = json.load(fp)

                            data_list.append(game_dict)

                        if os.path.isfile(feed_file):
                            os.remove(feed_file)

                        with open(feed_file, "w") as fp:
                            json.dump(data_list, fp, indent=2)
                        logger.warning(f"Done generating {feed_file}")

                        delete_games_for_season_and_feed(season, feed)
                        delete_weekly_feeds_for_season(season)

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


def get_data_for_week(week: int, season_start_year: int = None) -> None:
    week = str(week)
    if not season_start_year:
        now = datetime.today()
        season_start_year = now.year if now.month > 8 else now.year - 1

    season = f"{season_start_year}-{season_start_year + 1}-regular"
    logger.warning(f"Downloading data for week {week} of {season} season")
    if not get_feeds([WEEKLY_GAME_FEED], season, {"week": week}):
        for feed in BY_GAME_FEEDS:
            feed_file = get_filename(feed, season, {"week": week})
            if not os.path.isfile(feed_file):
                logger.warning(
                    f"Downloading {feed} game feeds for week {week} of {season} season"
                )
                with open(
                    get_filename(WEEKLY_GAME_FEED, season, {"week": week}), "r"
                ) as fp:
                    games = json.load(fp)["games"]
                errors = []
                for game in games:
                    (game_id, plusone_game_id, minusone_game_id) = get_game_ids(game)
                    errors += get_feeds(
                        [feed],
                        season,
                        {"game": game_id},
                        [{"game": plusone_game_id}, {"game": minusone_game_id}],
                    )

                if not errors:
                    logger.warning(
                        f"Retrieved all {feed} game feeds for week {week} of {season} season"
                    )
                    logger.warning(f"Generating {feed_file}")
                    data_list = []
                    for game in games:
                        with open(get_game_file(feed, season, game), "r") as fp:
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
