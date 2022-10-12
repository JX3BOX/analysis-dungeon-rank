from collections import Counter, defaultdict
from functools import partial
from itertools import chain
from operator import itemgetter
from typing import Any, DefaultDict, Dict, List

import pandas as pd
from scipy.stats import kstest, norm

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

RESULT: DefaultDict[str, Dict[str, Any]] = defaultdict(dict)

# https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/school.json
with open("school.json", "rb") as f:
    school = json.loads(f.read())


force_id_mounts = {
    force["force_id"]: force["mounts"] for force in school.values()
}

# https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/mount_group.json
with open("mount_group.json", "rb") as f:
    mount_group = json.loads(f.read())


def outlier_dropper(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    outlier_dropper

    Drop the outlier by norm or IQR .

    Args:
        df (pd.DataFrame): dataframe .
        col (str): filtered column .

    Returns:
        pd.DataFrame: outlier dropped dataframe .
    """
    mean, std = df[col].mean(), df[col].std()

    if kstest(df[col], cdf="norm", args=(mean, std)).pvalue > 0.05:
        low, high = norm.interval(0.95, loc=mean, scale=std)
    else:
        Q1 = df[col].quantile(0.25)
        Q3 = df[col].quantile(0.75)
        IQR = Q3 - Q1
        low = Q1 - IQR * 1.5
        high = Q3 + IQR * 1.5

    return df.query(f"{low} <= {col} <= {high}")


def dumper(output: str = "result.json") -> int:
    """
    dumper

    Dump to `JSON` file .
    """
    with open(output, mode="w+", encoding="utf-8") as f:
        return f.write(json.dumps(RESULT))


###############
# Statistics
###############


def top10_achieve_team_count(df: pd.DataFrame) -> None:
    """
    top10_achieve_team_count

    前 10 个击杀 boss 的团队区服统计
    """

    def _top10_achieve_team_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df.sort_values("finish_time", ascending=True)
            .head(10)
            .groupby("server", as_index=False)["team_id"]  # type: ignore
            .count()
            .sort_values("team_id", ascending=False)
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )

    RESULT["top10_achieve_team_count"]["all"] = _top10_achieve_team_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["top10_achieve_team_count"][  # type: ignore
            df.name
        ] = _top10_achieve_team_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def top100_achieve_team_count(df: pd.DataFrame) -> None:
    """
    top100_achieve_team_count

    前 100 个击杀 boss 的团队区服统计
    """

    def _top100_achieve_team_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df.sort_values("finish_time", ascending=True)
            .head(100)
            .groupby("server", as_index=False)["team_id"]  # type: ignore
            .count()
            .sort_values("team_id", ascending=False)
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )

    RESULT["top100_achieve_team_count"]["all"] = _top100_achieve_team_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["top100_achieve_team_count"][  # type: ignore
            df.name
        ] = _top100_achieve_team_count(df)

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
            df.groupby("server", as_index=False)["team_id"]  # type: ignore
            .count()
            .sort_values("team_id", ascending=False)
            .rename(columns={"server": "item", "team_id": "value"})
            .to_dict("list")
        )

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
        )

    RESULT["mount_attendance_count"]["all"] = _mount_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["mount_attendance_count"][  # type: ignore
            df.name
        ] = _mount_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def hps_count(df: pd.DataFrame) -> None:
    """
    hps_count

    治疗心法个数统计
    """

    def _hps_count(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("治疗", as_index=False)["team_id"]  # type: ignore
            .count()
            .sort_values("team_id", ascending=False)
            .rename(columns={"治疗": "item", "team_id": "value"})
            .to_dict("list")
        )

    RESULT["hps_count"]["all"] = _hps_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["hps_count"][df.name] = _hps_count(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def hps_attendance_count(df: pd.DataFrame) -> None:
    """
    hps_attendance_count

    治疗心法出场统计
    """

    def _hps_attendance_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df[mount_group["mount_group"]["治疗"]]
            .sum()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", 0: "value"})
            .to_dict("list")
        )

    RESULT["hps_attendance_count"]["all"] = _hps_attendance_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["hps_attendance_count"][  # type: ignore
            df.name
        ] = _hps_attendance_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def tank_count(df: pd.DataFrame) -> None:
    """
    tank_count

    防御心法个数统计
    """

    def _tank_count(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("坦克", as_index=False)["team_id"]  # type: ignore
            .count()
            .sort_values("team_id", ascending=False)
            .rename(columns={"坦克": "item", "team_id": "value"})
            .to_dict("list")
        )

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

    def _leader_mount_type_count(
        df: pd.DataFrame,
    ) -> Dict[str, List[str | int]]:
        return (
            df["mount_type"]
            .value_counts()
            .sort_values(ascending=False)
            .reset_index()
            .rename(columns={"index": "item", "mount_type": "value"})
            .to_dict("list")
        )  # type: ignore

    RESULT["leader_mount_type_count"]["all"] = _leader_mount_type_count(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["leader_mount_type_count"][  # type: ignore
            df.name
        ] = _leader_mount_type_count(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def flight_time_mean(df: pd.DataFrame) -> None:
    """
    flight_time_mean

    BOSS 平均战斗时长
    """
    RESULT["flight_time_mean"]["all"] = (
        df.groupby("achieve_id", as_index=False)["fight_time"]  # type: ignore
        .mean()
        .rename(columns={"achieve_id": "item", "fight_time": "value"})
        .to_dict("list")
    )


##########
# Rank
##########


def rank_mount_dps(df: pd.DataFrame) -> None:
    """
    rank_mount_dps

    输出心法平均 DPS
    """
    df = outlier_dropper(df.query("mount_type.isin(('外攻', '内攻'))"), "dps")

    def _rank_mount_dps(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("mount", as_index=False)["dps"]  # type: ignore
            .mean()
            .sort_values("dps", ascending=False)
            .rename(columns={"mount": "item", "dps": "value"})
            .to_dict("list")
        )

    RESULT["rank_mount_dps"]["all"] = _rank_mount_dps(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["rank_mount_dps"][df.name] = _rank_mount_dps(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def rank_mount_damage(df: pd.DataFrame) -> None:
    """
    rank_mount_damage

    输出心法平均伤害量
    """
    df = outlier_dropper(df.query("mount_type.isin(('外攻', '内攻'))"), "damage")

    def _rank_mount_damage(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("mount", as_index=False)["damage"]  # type: ignore
            .mean()
            .sort_values("damage", ascending=False)
            .rename(columns={"mount": "item", "damage": "value"})
            .to_dict("list")
        )

    RESULT["rank_mount_damage"]["all"] = _rank_mount_damage(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["rank_mount_damage"][  # type: ignore
            df.name
        ] = _rank_mount_damage(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def rank_mount_hps(df: pd.DataFrame) -> None:
    """
    rank_mount_hps

    治疗心法平均 HPS
    """
    df = outlier_dropper(df.query("mount_type == '治疗'"), "hps")

    def _rank_mount_hps(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("mount", as_index=False)["hps"]  # type: ignore
            .mean()
            .sort_values("hps", ascending=False)
            .rename(columns={"mount": "item", "hps": "value"})
            .to_dict("list")
        )

    RESULT["rank_mount_hps"]["all"] = _rank_mount_hps(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["rank_mount_hps"][df.name] = _rank_mount_hps(df)  # type: ignore

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


def rank_mount_therapy(df: pd.DataFrame) -> None:
    """
    rank_mount_therapy

    治疗心法平均治疗量
    """
    df = outlier_dropper(df.query("mount_type == '治疗'"), "therapy")

    def _rank_mount_therapy(df: pd.DataFrame) -> Dict[str, List[str | int]]:
        return (
            df.groupby("mount", as_index=False)["therapy"]  # type: ignore
            .mean()
            .sort_values("therapy", ascending=False)
            .rename(columns={"mount": "item", "therapy": "value"})
            .to_dict("list")
        )

    RESULT["rank_mount_therapy"]["all"] = _rank_mount_therapy(df)

    def agg_func(df: pd.DataFrame) -> None:
        RESULT["rank_mount_therapy"][  # type: ignore
            df.name
        ] = _rank_mount_therapy(df)

    df.groupby("achieve_id").apply(agg_func)  # type: ignore


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
            dtype={"guid": str},
        )
        .query("status == 1 & verified == 1")
        .drop(columns=["status", "verified"])
        .dropna(how="any")
        .reset_index(drop=True)
        .apply(
            pd.to_numeric, errors="ignore", downcast="unsigned"  # type: ignore
        )
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
            df["teammate"].map(  # type: ignore
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

    # escape the mount type from mount id
    df["mount_type"] = (
        df["mount"]
        .map(
            {
                mount_id: mount_type
                for mount_type, mount_ids in mount_group["mount_group"].items()
                for mount_id in mount_ids
            }
        )
        .apply(pd.to_numeric, errors="ignore", downcast="unsigned")
    )

    teams = df.query("is_leader == 1")

    ###############
    # Statistics
    ###############

    top10_achieve_team_count(teams)
    top100_achieve_team_count(teams)
    server_rank_team_count(teams)
    force_attendance_count(teams)
    mount_attendance_count(teams)
    hps_count(teams)
    hps_attendance_count(teams)
    tank_count(teams)
    tank_attendance_count(teams)
    dps_count(teams)
    mount_type_attendance_count(teams)
    leader_mount_type_count(teams)
    flight_time_mean(teams)

    ##########
    # Rank
    ##########

    rank_mount_dps(df)
    rank_mount_damage(df)
    rank_mount_hps(df)
    rank_mount_therapy(df)


if __name__ == "__main__":
    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument("-i", "--input", type=str, required=True)
    parser.add_argument("-o", "--output", type=str, default="result.json")

    args = parser.parse_args()

    analysis(args.input)

    dumper(args.output)
