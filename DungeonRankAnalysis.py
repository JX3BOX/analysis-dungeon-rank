import json
from argparse import ArgumentParser
from collections import Counter
from csv import DictReader
from operator import itemgetter
from typing import List


def IQR(lst: List[float | int], *, coefficient: float | int = 1.5) -> List[float | int]:
    lst = sorted(lst)

    idx = len(lst) // 4

    Q1 = lst[idx]
    Q3 = lst[-idx]

    IQR = Q3 - Q1

    return [i for i in lst if Q1 - coefficient * IQR <= i <= Q3 + coefficient * IQR]


def MAD(lst: List[float | int], *, coefficient: float | int = 2) -> List[float | int]:
    lst = sorted(lst)

    idx = len(lst) // 2

    median = lst[idx]

    error = sorted(i - median for i in lst)

    MAD = error[idx]

    return [
        i for i in lst if median - MAD * coefficient <= i <= median + MAD * coefficient
    ]


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("-i", "--input", type=str, required=True)
    parser.add_argument("-o", "--output", type=str, default="result.json")
    parser.add_argument("--boss", type=str, required=True)

    args = parser.parse_args()

    boss_lst = args.boss.split(",")

    with open("mount_group.json", "r+", encoding="utf-8") as f:
        mount_group = json.load(f)

    with open("school.json", "r+", encoding="utf-8") as f:
        school = json.load(f)

    mount_id_to_force_id = {
        mount: v["force_id"] for v in school.values() for mount in v["mounts"]
    }

    mount_id_to_mount_group = {
        mount_id: k for k, v in mount_group["mount_group"].items() for mount_id in v
    }

    mount_id_to_mount_type = {
        mount_id: k for k, v in mount_group["mount_types"].items() for mount_id in v
    }

    with open(args.input, "r+", encoding="utf-8") as f:
        data = sorted(
            (
                line
                for line in DictReader(f)
                if line["status"] == "1" and line["verified"] == "1"
            ),
            key=itemgetter("finish_time"),
        )

    for line in data:
        line["teammate"] = [
            dict(
                zip(
                    ("name", "mount_id", "global_role_id", "role_id"),
                    teammate.split(","),
                )
            )
            for teammate in line["teammate"].split(";")
        ]

        if line["mount"] == "10144":
            line["mount"] = "10145"

        for teammate in line["teammate"]:
            teammate["mount_id"] = int(teammate["mount_id"])
            if teammate["mount_id"] == 10144:
                teammate["mount_id"] = 10145

        line["mount_count"] = Counter(
            teammate["mount_id"] for teammate in line["teammate"]
        )

        line["force_count"] = Counter(
            mount_id_to_force_id[teammate["mount_id"]] for teammate in line["teammate"]
        )

        line["hps_count"] = sum(
            itemgetter(*mount_group["mount_group"]["治疗"])(line["mount_count"])
        )

        line["tank_count"] = sum(
            itemgetter(*mount_group["mount_group"]["坦克"])(line["mount_count"])
        )

        line["dps_count"] = sum(
            itemgetter(
                *mount_group["mount_group"]["外攻"], *mount_group["mount_group"]["内攻"]
            )(line["mount_count"])
        )

        line["外攻"] = sum(
            itemgetter(*mount_group["mount_group"]["外攻"])(line["mount_count"])
        )

        line["内攻"] = sum(
            itemgetter(*mount_group["mount_group"]["内攻"])(line["mount_count"])
        )

        line["mount"] = int(line["mount"])

    leader = [item for item in data if item["is_leader"] == "1"]

    response = {}

    ##################################
    # 前 10 个击杀 BOSS 的团队区服统计 #
    ##################################

    top10_achieve_team_count = {}

    top10_achieve_team_count_all = Counter(team["server"] for team in leader[:10])
    top10_achieve_team_count["all"] = {
        "item": list(map(itemgetter(0), top10_achieve_team_count_all.most_common())),
        "value": list(map(itemgetter(1), top10_achieve_team_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            [team["server"] for team in leader if team["achieve_id"] == boss][:10]
        )
        top10_achieve_team_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["top10_achieve_team_count"] = top10_achieve_team_count

    ###################################
    # 前 100 个击杀 BOSS 的团队区服统计 #
    ###################################

    top100_achieve_team_count = {}

    top100_achieve_team_count_all = Counter(team["server"] for team in leader[:100])
    top100_achieve_team_count["all"] = {
        "item": list(map(itemgetter(0), top100_achieve_team_count_all.most_common())),
        "value": list(map(itemgetter(1), top100_achieve_team_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            [team["server"] for team in leader if team["achieve_id"] == boss][:100]
        )
        top100_achieve_team_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["top100_achieve_team_count"] = top100_achieve_team_count

    ###################
    # 入榜团队数量统计 #
    ###################

    server_rank_team_count = {}

    server_rank_team_count_all = Counter(team["server"] for team in leader)
    server_rank_team_count["all"] = {
        "item": list(map(itemgetter(0), server_rank_team_count_all.most_common())),
        "value": list(map(itemgetter(1), server_rank_team_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            team["server"] for team in leader if team["achieve_id"] == boss
        )
        server_rank_team_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["server_rank_team_count"] = server_rank_team_count

    ###############
    # 门派出场统计 #
    ###############

    force_attendance_count = {}

    force_attendance_count_all = sum(
        (team["force_count"] for team in leader), start=Counter()
    )

    force_attendance_count["all"] = {
        "item": list(map(itemgetter(0), force_attendance_count_all.most_common())),
        "value": list(map(itemgetter(1), force_attendance_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = sum(
            (team["force_count"] for team in leader if team["achieve_id"] == boss),
            start=Counter(),
        )
        force_attendance_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["force_attendance_count"] = force_attendance_count

    ###############
    # 心法出场统计 #
    ###############

    mount_attendance_count = {}

    mount_attendance_count_all = sum(
        (team["mount_count"] for team in leader), start=Counter()
    )
    mount_attendance_count["all"] = {
        "item": list(map(itemgetter(0), mount_attendance_count_all.most_common())),
        "value": list(map(itemgetter(1), mount_attendance_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = sum(
            (team["mount_count"] for team in leader if team["achieve_id"] == boss),
            start=Counter(),
        )
        mount_attendance_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["mount_attendance_count"] = mount_attendance_count

    ###################
    # 治疗心法个数统计 #
    ###################

    hps_count = {}

    hps_count_all = Counter(team["hps_count"] for team in leader)
    hps_count["all"] = {
        "item": list(map(itemgetter(0), hps_count_all.most_common())),
        "value": list(map(itemgetter(1), hps_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            team["hps_count"] for team in leader if team["achieve_id"] == boss
        )
        hps_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["hps_count"] = hps_count

    # 全心法统计辅助计算
    mount_count_total = sum((team["mount_count"] for team in leader), start=Counter())

    ###################
    # 治疗心法出场统计 #
    ###################

    hps_attendance_count = {}

    hps_attendance_count_all = Counter(
        {
            mount_id: mount_count_total[mount_id]
            for mount_id in mount_group["mount_group"]["治疗"]
        }
    )

    hps_attendance_count["all"] = {
        "item": list(map(itemgetter(0), hps_attendance_count_all.most_common())),
        "value": list(map(itemgetter(1), hps_attendance_count_all.most_common())),
    }

    for boss in boss_lst:
        mount_count = sum(
            (team["mount_count"] for team in leader if team["achieve_id"] == boss),
            start=Counter(),
        )

        counter = Counter(
            {
                mount_id: mount_count[mount_id]
                for mount_id in mount_group["mount_group"]["治疗"]
            }
        )

        hps_attendance_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["hps_attendance_count"] = hps_attendance_count

    ###################
    # 防御心法个数统计 #
    ###################

    tank_count = {}

    tank_count_all = Counter(team["tank_count"] for team in leader)
    tank_count["all"] = {
        "item": list(map(itemgetter(0), tank_count_all.most_common())),
        "value": list(map(itemgetter(1), tank_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            team["tank_count"] for team in leader if team["achieve_id"] == boss
        )

        tank_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["tank_count"] = tank_count

    ###################
    # 防御心法出场统计 #
    ###################

    tank_attendance_count = {}

    tank_attendance_count_all = Counter(
        {
            mount_id: mount_count_total[mount_id]
            for mount_id in mount_group["mount_group"]["坦克"]
        }
    )

    tank_attendance_count["all"] = {
        "item": list(map(itemgetter(0), tank_attendance_count_all.most_common())),
        "value": list(map(itemgetter(1), tank_attendance_count_all.most_common())),
    }

    for boss in boss_lst:
        mount_count = sum(
            (team["mount_count"] for team in leader if team["achieve_id"] == boss),
            start=Counter(),
        )

        counter = Counter(
            {
                mount_id: mount_count[mount_id]
                for mount_id in mount_group["mount_group"]["坦克"]
            }
        )

        tank_attendance_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["tank_attendance_count"] = tank_attendance_count

    ###############
    # 输出心法统计 #
    ###############

    dps_count = {}

    dps_count_all = Counter(
        {
            mount_id: mount_count_total[mount_id]
            for mount_id in (
                mount_group["mount_group"]["外攻"] + mount_group["mount_group"]["内攻"]
            )
        }
    )

    dps_count["all"] = {
        "item": list(map(itemgetter(0), dps_count_all.most_common())),
        "value": list(map(itemgetter(1), dps_count_all.most_common())),
    }

    for boss in boss_lst:
        mount_count = sum(
            (team["mount_count"] for team in leader if team["achieve_id"] == boss),
            start=Counter(),
        )

        counter = Counter(
            {
                mount_id: mount_count[mount_id]
                for mount_id in (
                    mount_group["mount_group"]["外攻"]
                    + mount_group["mount_group"]["内攻"]
                )
            }
        )

        dps_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["dps_count"] = dps_count

    #################
    # 内外功出场统计 #
    #################

    mount_type_attendance_count = {}

    mount_type_attendance_count_all = Counter(
        {
            "外攻": sum(team["外攻"] for team in leader),
            "内攻": sum(team["内攻"] for team in leader),
        }
    )

    mount_type_attendance_count["all"] = {
        "item": list(map(itemgetter(0), mount_type_attendance_count_all.most_common())),
        "value": list(
            map(itemgetter(1), mount_type_attendance_count_all.most_common())
        ),
    }

    for boss in boss_lst:
        counter = Counter(
            {
                "外攻": sum(
                    team["外攻"] for team in leader if team["achieve_id"] == boss
                ),
                "内攻": sum(
                    team["内攻"] for team in leader if team["achieve_id"] == boss
                ),
            }
        )

        mount_type_attendance_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["mount_type_attendance_count"] = mount_type_attendance_count

    ###################
    # 团长心法类型统计 #
    ###################

    leader_mount_type_count = {}

    leader_mount_type_count_all = Counter(
        mount_id_to_mount_group[team["mount"]] for team in leader
    )

    leader_mount_type_count["all"] = {
        "item": list(map(itemgetter(0), leader_mount_type_count_all.most_common())),
        "value": list(map(itemgetter(1), leader_mount_type_count_all.most_common())),
    }

    for boss in boss_lst:
        counter = Counter(
            mount_id_to_mount_group[team["mount"]]
            for team in leader
            if team["achieve_id"] == boss
        )

        leader_mount_type_count[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["leader_mount_type_count"] = leader_mount_type_count

    ###################
    # 输出心法平均 DPS #
    ###################

    rank_mount_dps = {}

    rank_mount_dps_all = {}
    for mount_id in (
        mount_group["mount_group"]["外攻"] + mount_group["mount_group"]["内攻"]
    ):
        if lst := [
            float(team["dps"])
            for team in data
            if team["dps"] and team["mount"] == mount_id
        ]:
            dps = IQR(lst)

            rank_mount_dps_all[mount_id] = sum(dps) / len(dps)

    rank_mount_dps_all = Counter(rank_mount_dps_all)

    rank_mount_dps["all"] = {
        "item": list(map(itemgetter(0), rank_mount_dps_all.most_common())),
        "value": list(map(itemgetter(1), rank_mount_dps_all.most_common())),
    }

    for boss in boss_lst:
        mount_dps = {}

        for mount_id in (
            mount_group["mount_group"]["外攻"] + mount_group["mount_group"]["内攻"]
        ):
            if lst := [
                float(team["dps"])
                for team in data
                if team["dps"]
                and team["mount"] == mount_id
                and team["achieve_id"] == boss
            ]:
                dps = IQR(lst)

                mount_dps[mount_id] = sum(dps) / len(dps)

        counter = Counter(mount_dps)

        rank_mount_dps[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["rank_mount_dps"] = rank_mount_dps

    #####################
    # 输出心法平均伤害量 #
    #####################

    rank_mount_damage = {}

    rank_mount_damage_all = {}
    for mount_id in (
        mount_group["mount_group"]["外攻"] + mount_group["mount_group"]["内攻"]
    ):
        if lst := [
            float(team["damage"])
            for team in data
            if team["damage"] and team["mount"] == mount_id
        ]:
            damage = IQR(lst)

            rank_mount_damage_all[mount_id] = sum(damage) / len(damage)

    rank_mount_damage_all = Counter(rank_mount_damage_all)

    rank_mount_damage["all"] = {
        "item": list(map(itemgetter(0), rank_mount_damage_all.most_common())),
        "value": list(map(itemgetter(1), rank_mount_damage_all.most_common())),
    }

    for boss in boss_lst:
        mount_damage = {}

        for mount_id in (
            mount_group["mount_group"]["外攻"] + mount_group["mount_group"]["内攻"]
        ):
            if lst := [
                float(team["damage"])
                for team in data
                if team["damage"]
                and team["mount"] == mount_id
                and team["achieve_id"] == boss
            ]:
                damage = IQR(lst)

                mount_damage[mount_id] = sum(damage) / len(damage)

        counter = Counter(mount_damage)

        rank_mount_damage[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["rank_mount_damage"] = rank_mount_damage

    ###################
    # 治疗心法平均 HPS #
    ###################

    rank_mount_hps = {}

    rank_mount_hps_all = {}
    for mount_id in mount_group["mount_group"]["治疗"]:
        if lst := [
            float(team["hps"])
            for team in data
            if team["hps"] and team["mount"] == mount_id
        ]:
            hps = IQR(lst)

            rank_mount_hps_all[mount_id] = sum(hps) / len(hps)

    rank_mount_hps_all = Counter(rank_mount_hps_all)

    rank_mount_hps["all"] = {
        "item": list(map(itemgetter(0), rank_mount_hps_all.most_common())),
        "value": list(map(itemgetter(1), rank_mount_hps_all.most_common())),
    }

    for boss in boss_lst:
        mount_hps = {}

        for mount_id in mount_group["mount_group"]["治疗"]:
            if lst := [
                float(team["hps"])
                for team in data
                if team["hps"]
                and team["mount"] == mount_id
                and team["achieve_id"] == boss
            ]:
                hps = IQR(lst)

                mount_hps[mount_id] = sum(hps) / len(hps)

        counter = Counter(mount_hps)

        rank_mount_hps[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["rank_mount_hps"] = rank_mount_hps

    #####################
    # 治疗心法平均治疗量 #
    #####################

    rank_mount_therapy = {}

    rank_mount_therapy_all = {}
    for mount_id in mount_group["mount_group"]["治疗"]:
        if lst := [
            float(team["therapy"])
            for team in data
            if team["therapy"] and team["mount"] == mount_id
        ]:
            hps = IQR(lst)

            rank_mount_therapy_all[mount_id] = sum(hps) / len(hps)

    rank_mount_therapy_all = Counter(rank_mount_therapy_all)

    rank_mount_therapy["all"] = {
        "item": list(map(itemgetter(0), rank_mount_therapy_all.most_common())),
        "value": list(map(itemgetter(1), rank_mount_therapy_all.most_common())),
    }

    for boss in boss_lst:
        mount_therapy = {}

        for mount_id in mount_group["mount_group"]["治疗"]:
            if lst := [
                float(team["therapy"])
                for team in data
                if team["therapy"]
                and team["mount"] == mount_id
                and team["achieve_id"] == boss
            ]:
                therapy = IQR(lst)

                mount_therapy[mount_id] = sum(therapy) / len(therapy)

        counter = Counter(mount_therapy)

        rank_mount_therapy[boss] = {
            "item": list(map(itemgetter(0), counter.most_common())),
            "value": list(map(itemgetter(1), counter.most_common())),
        }

    response["rank_mount_therapy"] = rank_mount_therapy

    ########
    # dump #
    ########
    with open(args.output, "w+", encoding="utf-8") as f:
        f.write(
            json.dumps(
                response, ensure_ascii=False, separators=(",", ":"), sort_keys=False
            )
        )
