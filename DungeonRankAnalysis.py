from collections import Counter, defaultdict
from functools import partial
from itertools import chain
from operator import itemgetter
from typing import Any, DefaultDict, Dict, List

import pandas as pd

try:
    import ujson as json

    json.dumps = partial(json.dumps, ensure_ascii=False, sort_keys=False)

except ImportError:
    import json

    json.dumps = partial(
        json.dumps, ensure_ascii=False, separators=(",", ":"), sort_keys=False
    )


###############
# Data Dict
###############

# https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/school.json
with open("school.json", "rb") as f:
    school = json.loads(f.read())


force_id_mounts = {
    force["force_id"]: force["mounts"] for force in school.values()
}

# https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/mount_group.json
with open("mount_group.json", "rb") as f:
    mount_group = json.loads(f.read())


RESULT: DefaultDict[str, Dict[str, Any]] = defaultdict(dict)


####################
# Analysis Function
####################


def top10_server_team_count(df: pd.DataFrame) -> None:
    """
    top10_server_team_count

    前 10 个击杀 boss 的团队区服统计
    """

    def _top10_achieve_team_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df.sort_values("finish_time", ascending=True)
            .head(10)
            .groupby("server")["team_id"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["top10_achieve_team_count"]["all"] = _top10_achieve_team_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["top10_achieve_team_count"][  # type: ignore
            df.name
        ] = _top10_achieve_team_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def top100_server_team_count(df: pd.DataFrame) -> None:
    """
    top100_server_team_count

    前 100 个击杀 boss 的团队区服统计
    """

    def _top100_server_team_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df.sort_values("finish_time", ascending=True)
            .head(100)
            .groupby("server")["team_id"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["top100_server_team_count"]["all"] = _top100_server_team_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["top100_server_team_count"][  # type: ignore
            df.name
        ] = _top100_server_team_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def server_rank_team_count(df: pd.DataFrame) -> None:
    """
    server_rank_team_count

    入榜团队数量统计
    """

    def _server_rank_team_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df.groupby("server")["team_id"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["server_rank_team_count"]["all"] = _server_rank_team_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["server_rank_team_count"][  # type: ignore
            df.name
        ] = _server_rank_team_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def force_attendance_count(df: pd.DataFrame) -> None:
    """
    force_attendance_count

    门派出场统计
    """

    def _force_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[map(itemgetter("force_id"), school.values())]  # type: ignore
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )

    RESULT["force_attendance_count"]["all"] = _force_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["force_attendance_count"][  # type: ignore
            df.name
        ] = _force_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def mount_attendance_count(df: pd.DataFrame) -> None:
    """
    mount_attendance_count

    心法出场统计
    """

    def _mount_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[
                chain.from_iterable(map(itemgetter("mounts"), school.values()))
            ]  # type: ignore
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["mount_attendance_count"]["all"] = _mount_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["mount_attendance_count"][  # type: ignore
            df.name
        ] = _mount_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def heal_count(df: pd.DataFrame) -> None:
    """
    heal_count

    治疗心法个数统计
    """

    def _heal_count(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("治疗")["team_id"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"治疗": "item", "team_id": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["heal_count"]["all"] = _heal_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["heal_count"][df.name] = _heal_count(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def heal_attendance_count(df: pd.DataFrame) -> None:
    """
    heal_attendance_count

    治疗心法出场统计
    """

    def _heal_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[mount_group["mount_group"]["治疗"]]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["heal_attendance_count"]["all"] = _heal_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["heal_attendance_count"][  # type: ignore
            df.name
        ] = _heal_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def tank_count(df: pd.DataFrame) -> None:
    """
    tank_count

    防御心法个数统计
    """

    def _tank_count(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("坦克")["team_id"]
            .count()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"坦克": "item", "team_id": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["tank_count"]["all"] = _tank_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["tank_count"][df.name] = _tank_count(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def tank_attendance_count(df: pd.DataFrame) -> None:
    """
    tank_attendance_count

    防御心法出场统计
    """

    def _tank_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[mount_group["mount_group"]["坦克"]]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["tank_attendance_count"]["all"] = _tank_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["tank_attendance_count"][  # type: ignore
            df.name
        ] = _tank_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def dps_count(df: pd.DataFrame) -> None:
    """
    dps_count

    输出心法统计
    """

    def _dps_count(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df[
                mount_group["mount_group"]["外攻"]
                + mount_group["mount_group"]["内攻"]
            ]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["dps_count"]["all"] = _dps_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["dps_count"][df.name] = _dps_count(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def mount_type_attendance_count(df: pd.DataFrame) -> None:
    """
    mount_type_attendance_count

    内外功出场统计
    """

    def _mount_type_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[["外攻", "内攻"]]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["mount_type_attendance_count"][
        "all"
    ] = _mount_type_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["mount_type_attendance_count"][  # type: ignore
            df.name
        ] = _mount_type_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def leader_mount_type_count(df: pd.DataFrame) -> None:
    """
    leader_mount_type_count

    团长心法类型统计
    """
    RESULT["leader_mount_type_count"] = (  # type: ignore
        df["mount"]
        .map(
            {
                mount_id: mount_type
                for mount_type, mount_ids in mount_group["mount_group"].items()
                for mount_id in mount_ids
            }
        )
        .value_counts()
        .sort_values(ascending=False)
        .reset_index()
        .rename(columns={"index": "item", "mount": "value"})
        .to_dict("list")
    )


def flight_time_mean(df: pd.DataFrame) -> None:
    """
    flight_time_mean

    BOSS 平均战斗时长
    """
    RESULT["flight_time_mean"] = (  # type: ignore
        df.groupby("achieve_id")["fight_time"]
        .mean()
        .reset_index()
        .rename(columns={"achieve_id": "item", "fight_time": "value"})
        .to_dict("list")
    )


def dumper(output: str = "result.json") -> int:
    with open(output, mode="w+", encoding="utf-8") as f:
        return f.write(json.dumps(RESULT))


def analysis(file: str) -> None:
    #########################
    # Load the original csv
    #########################

    df = (
        pd.read_csv(
            file,
            usecols=(
                "guid",
                "server",
                "role",
                "leader",
                "teammate",
                "teammate_md5",
                "achieve_id",
                "finish_time",
                "start_time",
                "fight_time",
                "damage",
                "dps",
                "therapy",
                "hps",
                "body_type",
                "is_leader",
                "uid",
                "team_id",
                "status",
                "verified",
                "battleId",
                "mount",
            ),
        )
        .query("status == 1 & verified == 1")
        .reset_index(drop=True)
        .drop(columns=["status", "verified"])
        .apply(
            pd.to_numeric, errors="ignore", downcast="unsigned"
        )  # type: ignore
    )

    # df["finish_time"] = pd.to_datetime(df["finish_time"], unit="s")
    # df["start_time"] = pd.to_datetime(df["start_time"], unit="s")

    # df["zone"] = df["battleId"].map(lambda x: x.split("::")[1].split("_")[0])

    ##############################
    # Wide Table preprocessing
    ##############################

    # escape the mount id of teammates
    df = df.join(
        pd.json_normalize(
            df["teammate"].map(
                lambda x: Counter(
                    map(lambda x: int(x.split(",")[1]), x.split(";"))
                )
            )
        )
        .fillna(0)
        .apply(
            pd.to_numeric, errors="ignore", downcast="unsigned"
        )  # type: ignore
    )

    # count the mount type of teammates
    df = df.join(
        pd.concat(
            (
                df[v].apply(sum, axis=1)
                for v in mount_group["mount_group"].values()
            ),
            axis=1,
            keys=mount_group["mount_group"],
        ).apply(
            pd.to_numeric, errors="ignore", downcast="unsigned"
        )  # type: ignore
    )

    # count the force of teammates
    df = df.join(
        pd.concat(
            (
                df[mounts].apply(sum, axis=1)
                for mounts in force_id_mounts.values()
            ),
            axis=1,
            keys=force_id_mounts,
        ).apply(
            pd.to_numeric, errors="ignore", downcast="unsigned"
        )  # type: ignore
    )
    teams = df.query("is_leader == 1")

    ##########
    # Analysis
    ##########
    top10_server_team_count(teams)
    top100_server_team_count(teams)
    server_rank_team_count(teams)
    force_attendance_count(teams)
    mount_attendance_count(teams)
    heal_count(teams)
    heal_attendance_count(teams)
    tank_count(teams)
    tank_attendance_count(teams)
    dps_count(teams)
    mount_type_attendance_count(teams)
    leader_mount_type_count(teams)
    flight_time_mean(teams)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument("-i", "--input", type=str)
    parser.add_argument("-o", "--output", type=str, default="result.json")

    args = parser.parse_args()

    analysis(args.input)

    dumper(args.output)
