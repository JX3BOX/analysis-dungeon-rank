import json
from argparse import ArgumentParser

import duckdb


def rank(input: str, output: str, boss: str) -> None:
    boss_lst = boss.split(",")

    # width table

    #######
    # raw #
    #######
    duckdb.sql(
        f"""
        SELECT
            guid, server, role, leader, teammate, STRING_SPLIT(teammate, ';') AS teammate_lst,
            teammate_md5, achieve_id, finish_time, start_time, fight_time, damage, dps, therapy,
            hps, body_type, is_leader, uid, team_id, status, verified, battleId, mount,
            (
                CASE mount
                    WHEN 10002 THEN 1
                    WHEN 10003 THEN 1
                    WHEN 10021 THEN 2
                    WHEN 10028 THEN 2
                    WHEN 10026 THEN 3
                    WHEN 10062 THEN 3
                    WHEN 10014 THEN 4
                    WHEN 10015 THEN 4
                    WHEN 10080 THEN 5
                    WHEN 10081 THEN 5
                    WHEN 10175 THEN 6
                    WHEN 10176 THEN 6
                    WHEN 10224 THEN 7
                    WHEN 10225 THEN 7
                    WHEN 10144 THEN 8
                    WHEN 10145 THEN 8
                    WHEN 10268 THEN 9
                    WHEN 10242 THEN 10
                    WHEN 10243 THEN 10
                    WHEN 10389 THEN 21
                    WHEN 10390 THEN 21
                    WHEN 10447 THEN 22
                    WHEN 10448 THEN 22
                    WHEN 10464 THEN 23
                    WHEN 10533 THEN 24
                    WHEN 10585 THEN 25
                    WHEN 10615 THEN 211
                    WHEN 10627 THEN 212
                    WHEN 10626 THEN 212
                    WHEN 10698 THEN 213
                    WHEN 10756 THEN 214
                END
            ) AS force_id,
            (
                CASE
                    WHEN mount IN (10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756) THEN '外攻'
                    WHEN mount IN (10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627) THEN '内攻'
                    WHEN mount IN (10002, 10062, 10243, 10389) THEN '坦克'
                    WHEN mount IN (10448, 10176, 10028, 10080, 10626) THEN '治疗'
                END
            ) AS mount_type
        FROM READ_CSV('{input}')
        WHERE status = 1 AND verified = 1
        """
    ).to_view("raw", replace=True)

    ## top 100 of all
    duckdb.sql(
        """
        SELECT * FROM raw WHERE is_leader = 1
        QUALIFY RANK() OVER(ORDER BY finish_time ASC) <= 100
        """
    ).to_view("team_all", replace=True)

    ## top 100 of each boss
    duckdb.sql(
        """
        SELECT * FROM raw WHERE is_leader = 1
        QUALIFY RANK() OVER(PARTITION BY achieve_id ORDER BY finish_time ASC) <= 100
        """
    ).to_view("team_each_boss", replace=True)

    ## explode the teammate from field `teammate`
    duckdb.sql(
        """
        SELECT *, UNNEST(teammate_lst) AS teammate_explode FROM team_all
        """
    ).to_view("team_all_explode", replace=True)

    ## explode the teammate from field `teammate`
    duckdb.sql(
        """
        SELECT *, UNNEST(teammate_lst) AS teammate_explode FROM team_each_boss
        """
    ).to_view("team_each_boss_explode", replace=True)

    ## extract the information from exploded teammate
    duckdb.sql(
        """
        SELECT
            *,
            STRING_SPLIT(teammate_explode, ',')[1] AS teammate_name,
            STRING_SPLIT(teammate_explode, ',')[2] AS teammate_mount,
            STRING_SPLIT(teammate_explode, ',')[3] AS teammate_globalroleid,
            STRING_SPLIT(teammate_explode, ',')[4] AS teammate_roleid,
            (
                CASE STRING_SPLIT(teammate_explode, ',')[2]
                    WHEN 10002 THEN 1
                    WHEN 10003 THEN 1
                    WHEN 10021 THEN 2
                    WHEN 10028 THEN 2
                    WHEN 10026 THEN 3
                    WHEN 10062 THEN 3
                    WHEN 10014 THEN 4
                    WHEN 10015 THEN 4
                    WHEN 10080 THEN 5
                    WHEN 10081 THEN 5
                    WHEN 10175 THEN 6
                    WHEN 10176 THEN 6
                    WHEN 10224 THEN 7
                    WHEN 10225 THEN 7
                    WHEN 10144 THEN 8
                    WHEN 10145 THEN 8
                    WHEN 10268 THEN 9
                    WHEN 10242 THEN 10
                    WHEN 10243 THEN 10
                    WHEN 10389 THEN 21
                    WHEN 10390 THEN 21
                    WHEN 10447 THEN 22
                    WHEN 10448 THEN 22
                    WHEN 10464 THEN 23
                    WHEN 10533 THEN 24
                    WHEN 10585 THEN 25
                    WHEN 10615 THEN 211
                    WHEN 10627 THEN 212
                    WHEN 10626 THEN 212
                    WHEN 10698 THEN 213
                    WHEN 10756 THEN 214
                END
            ) AS teammate_force_id,
            (
                CASE
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756) THEN '外攻'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627) THEN '内攻'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10002, 10062, 10243, 10389) THEN '坦克'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10448, 10176, 10028, 10080, 10626) THEN '治疗'
                END
            ) AS teammate_mount_type
        FROM
            team_all_explode
        """
    ).to_view("team_all_mount_explode", replace=True)

    ## extract the information from exploded teammate
    duckdb.sql(
        """
        SELECT
            *,
            STRING_SPLIT(teammate_explode, ',')[1] AS teammate_name,
            STRING_SPLIT(teammate_explode, ',')[2] AS teammate_mount,
            STRING_SPLIT(teammate_explode, ',')[3] AS teammate_globalroleid,
            STRING_SPLIT(teammate_explode, ',')[4] AS teammate_roleid,
            (
                CASE STRING_SPLIT(teammate_explode, ',')[2]
                    WHEN 10002 THEN 1
                    WHEN 10003 THEN 1
                    WHEN 10021 THEN 2
                    WHEN 10028 THEN 2
                    WHEN 10026 THEN 3
                    WHEN 10062 THEN 3
                    WHEN 10014 THEN 4
                    WHEN 10015 THEN 4
                    WHEN 10080 THEN 5
                    WHEN 10081 THEN 5
                    WHEN 10175 THEN 6
                    WHEN 10176 THEN 6
                    WHEN 10224 THEN 7
                    WHEN 10225 THEN 7
                    WHEN 10144 THEN 8
                    WHEN 10145 THEN 8
                    WHEN 10268 THEN 9
                    WHEN 10242 THEN 10
                    WHEN 10243 THEN 10
                    WHEN 10389 THEN 21
                    WHEN 10390 THEN 21
                    WHEN 10447 THEN 22
                    WHEN 10448 THEN 22
                    WHEN 10464 THEN 23
                    WHEN 10533 THEN 24
                    WHEN 10585 THEN 25
                    WHEN 10615 THEN 211
                    WHEN 10627 THEN 212
                    WHEN 10626 THEN 212
                    WHEN 10698 THEN 213
                    WHEN 10756 THEN 214
                END
            ) AS teammate_force_id,
            (
                CASE
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756) THEN '外攻'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627) THEN '内攻'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10002, 10062, 10243, 10389) THEN '坦克'
                    WHEN STRING_SPLIT(teammate_explode, ',')[2] IN (10448, 10176, 10028, 10080, 10626) THEN '治疗'
                END
            ) AS teammate_mount_type
        FROM
            team_each_boss_explode
        """
    ).to_view("team_each_boss_mount_explode", replace=True)

    ## auxiliary width table of all
    duckdb.sql(
        """
        SELECT * FROM team_all AS e
        INNER JOIN (
            PIVOT team_all_mount_explode ON teammate_mount USING COUNT(DISTINCT teammate_globalroleid) GROUP BY teammate_md5
        ) AS t
        ON e.teammate_md5 = t.teammate_md5
        """
    ).to_view("team_all_with_teammate_mount_count")

    ## auxiliary width table of each boss
    duckdb.sql(
        """
        SELECT * FROM team_each_boss AS e
        INNER JOIN (
            PIVOT team_each_boss_mount_explode ON teammate_mount USING COUNT(DISTINCT teammate_globalroleid) GROUP BY teammate_md5
        ) AS t
        ON e.teammate_md5 = t.teammate_md5
        """
    ).to_view("team_each_boss_with_teammate_mount_count")

    ############
    # analysis #
    ############
    result = {}

    ##################################
    # 前 10 个击杀 BOSS 的团队区服统计 #
    ##################################
    result["top10_achieve_team_count"] = {}

    # all
    result["top10_achieve_team_count"]["all"] = (
        duckdb.sql(
            """
            WITH team AS (
                SELECT * FROM team_all QUALIFY RANK() OVER(ORDER BY finish_time ASC) <= 10
            )
            SELECT server AS item, COUNT(*) AS value FROM team GROUP BY server ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["top10_achieve_team_count"][boss_id] = (
            duckdb.sql(
                f"""
                WITH team AS (
                    SELECT * FROM team_each_boss
                    WHERE achieve_id = {boss_id}
                    QUALIFY RANK() OVER(ORDER BY finish_time ASC) <= 10
                )
                SELECT server AS item, COUNT(*) AS value FROM team GROUP BY server ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###################################
    # 前 100 个击杀 BOSS 的团队区服统计 #
    ###################################
    result["top100_achieve_team_count"] = {}

    # all
    result["top100_achieve_team_count"]["all"] = (
        duckdb.sql(
            """
            WITH team AS (
                SELECT * FROM team_all QUALIFY RANK() OVER(ORDER BY finish_time ASC) <= 100
            )
            SELECT server AS item, COUNT(*) AS value FROM team GROUP BY server ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["top100_achieve_team_count"][boss_id] = (
            duckdb.sql(
                f"""
                WITH team AS (
                    SELECT * FROM team_each_boss
                    WHERE achieve_id = {boss_id}
                    QUALIFY RANK() OVER(ORDER BY finish_time ASC) <= 100
                )
                SELECT server AS item, COUNT(*) AS value FROM team GROUP BY server ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###################
    # 入榜团队数量统计 #
    ###################
    result["server_rank_team_count"] = {}

    # all
    result["server_rank_team_count"]["all"] = (
        duckdb.sql(
            """
            SELECT server AS item, COUNT(*) AS value FROM team_all
            GROUP BY server
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["server_rank_team_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT server AS item, COUNT(*) AS value FROM team_each_boss
                WHERE achieve_id = {boss_id}
                GROUP BY server
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###############
    # 门派出场统计 #
    ###############
    result["force_attendance_count"] = {}

    # all
    result["force_attendance_count"]["all"] = (
        duckdb.sql(
            """
            SELECT teammate_force_id AS item, COUNT(*) AS value
            FROM team_all_mount_explode
            GROUP BY teammate_force_id
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["force_attendance_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT teammate_force_id AS item, COUNT(*) AS value
                FROM team_each_boss_mount_explode
                WHERE achieve_id = {boss_id}
                GROUP BY teammate_force_id
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###############
    # 心法出场统计 #
    ###############
    result["mount_attendance_count"] = {}

    # all
    result["mount_attendance_count"]["all"] = (
        duckdb.sql(
            """
            SELECT teammate_mount AS item, COUNT(*) AS value
            FROM team_all_mount_explode
            GROUP BY teammate_mount
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["mount_attendance_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT teammate_mount AS item, COUNT(*) AS value
                FROM team_each_boss_mount_explode
                WHERE achieve_id = {boss_id}
                GROUP BY teammate_mount
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###################
    # 治疗心法个数统计 #
    ###################
    result["hps_count"] = {}

    # all
    result["hps_count"]["all"] = (
        duckdb.sql(
            """
            WITH t AS (
                SELECT ("10448" + "10176" + "10028" + "10080" + "10626") AS hps_count
                FROM team_all_with_teammate_mount_count
            )
                SELECT hps_count AS item, COUNT(*) AS value
                FROM t
                GROUP BY hps_count
                ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["hps_count"][boss_id] = (
            duckdb.sql(
                f"""
                WITH t AS (
                    SELECT ("10448" + "10176" + "10028" + "10080" + "10626") AS hps_count
                    FROM team_each_boss_with_teammate_mount_count
                    WHERE achieve_id = {boss_id}
                )
                    SELECT hps_count AS item, COUNT(*) AS value
                    FROM t
                    GROUP BY hps_count
                    ORDER BY COUNT(*) DESC
                    """
            )
            .df()
            .to_dict("list")
        )

    ###################
    # 治疗心法出场统计 #
    ###################
    result["hps_attendance_count"] = {}

    # all
    result["hps_attendance_count"]["all"] = (
        duckdb.sql(
            """
            SELECT teammate_mount AS item, COUNT(*) AS value
            FROM team_all_mount_explode
            WHERE teammate_mount IN (10448, 10176, 10028, 10080, 10626)
            GROUP BY teammate_mount
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["hps_attendance_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT teammate_mount AS item, COUNT(*) AS value
                FROM team_each_boss_mount_explode
                WHERE teammate_mount IN (10448, 10176, 10028, 10080, 10626)
                    AND achieve_id = {boss_id}
                GROUP BY teammate_mount
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###################
    # 防御心法个数统计 #
    ###################
    result["tank_count"] = {}

    # all
    result["tank_count"]["all"] = (
        duckdb.sql(
            """
            WITH t AS (
                SELECT ("10002" + "10062" + "10243" + "10389") AS tank_count
                FROM team_all_with_teammate_mount_count
            )
                SELECT tank_count AS item, COUNT(*) AS value
                FROM t
                GROUP BY tank_count
                ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["tank_count"][boss_id] = (
            duckdb.sql(
                f"""
                WITH t AS (
                    SELECT ("10002" + "10062" + "10243" + "10389") AS tank_count
                    FROM team_each_boss_with_teammate_mount_count
                    WHERE achieve_id = {boss_id}
                )
                    SELECT tank_count AS item, COUNT(*) AS value
                    FROM t
                    GROUP BY tank_count
                    ORDER BY COUNT(*) DESC
                    """
            )
            .df()
            .to_dict("list")
        )

    ###################
    # 防御心法出场统计 #
    ###################
    result["tank_attendance_count"] = {}

    # all
    result["tank_attendance_count"]["all"] = (
        duckdb.sql(
            """
            SELECT teammate_mount AS item, COUNT(*) AS value
            FROM team_all_mount_explode
            WHERE teammate_mount IN (10002, 10062, 10243, 10389)
            GROUP BY teammate_mount
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["tank_attendance_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT teammate_mount AS item, COUNT(*) AS value
                FROM team_each_boss_mount_explode
                WHERE teammate_mount IN (10002, 10062, 10243, 10389)
                    AND achieve_id = {boss_id}
                GROUP BY teammate_mount
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    ###############
    # 输出心法统计 #
    ###############
    result["dps_count"] = {}

    # all
    result["dps_count"]["all"] = (
        duckdb.sql(
            """
            SELECT teammate_mount AS item, COUNT(*) AS value
            FROM team_all_mount_explode
            WHERE teammate_mount IN (
                10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
            )
            GROUP BY teammate_mount
            ORDER BY COUNT(*) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["dps_count"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT teammate_mount AS item, COUNT(*) AS value
                FROM team_each_boss_mount_explode
                WHERE teammate_mount IN (
                    10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                    10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
                ) AND achieve_id = {boss_id}
                GROUP BY teammate_mount
                ORDER BY COUNT(*) DESC
                """
            )
            .df()
            .to_dict("list")
        )

    #################
    # 内外功出场统计 #
    #################
    result["mount_type_attendance_count"] = {}

    # all

    d = (
        duckdb.sql(
            """
            SELECT
                SUM(CASE WHEN teammate_mount IN (10026, 10015, 10062, 10144, 10145, 10224, 10268, 10389, 10390, 10464, 10533, 10585, 10698, 10756) THEN 1 ELSE 0 END) AS '外攻',
                SUM(CASE WHEN teammate_mount IN (10002, 10003, 10021, 10175, 10014, 10081, 10225,  10242, 10243, 10447, 10615, 10627, 10448, 10176, 10028, 10080, 10626) THEN 1 ELSE 0 END) AS '内攻'
            FROM team_all_mount_explode
            """
        )
        .df()
        .to_dict("records")[0]
    )

    result["mount_type_attendance_count"]["all"] = {
        "item": list(d.keys()),
        "value": list(d.values()),
    }

    # each boss
    for boss_id in boss_lst:
        d = (
            duckdb.sql(
                f"""
                SELECT
                    SUM(CASE WHEN teammate_mount IN (10026, 10015, 10062, 10144, 10145, 10224, 10268, 10389, 10390, 10464, 10533, 10585, 10698, 10756) THEN 1 ELSE 0 END) AS '外攻',
                    SUM(CASE WHEN teammate_mount IN (10002, 10003, 10021, 10175, 10014, 10081, 10225,  10242, 10243, 10447, 10615, 10627, 10448, 10176, 10028, 10080, 10626) THEN 1 ELSE 0 END) AS '内攻'
                FROM team_each_boss_mount_explode
                WHERE achieve_id = {boss_id}
            """
            )
            .df()
            .to_dict("records")[0]
        )

        result["mount_type_attendance_count"][boss_id] = {
            "item": list(d.keys()),
            "value": list(d.values()),
        }

    ###################
    # 团长心法类型统计 #
    ###################
    result["leader_mount_type_count"] = {}

    # all
    d = (
        duckdb.sql(
            """
            SELECT
                SUM(CASE WHEN mount IN (10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756) THEN 1 ELSE 0 END) AS '外攻',
                SUM(CASE WHEN mount IN (10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627) THEN 1 ELSE 0 END) AS '内攻',
                SUM(CASE WHEN mount IN (10002, 10062, 10243, 10389) THEN 1 ELSE 0 END) AS '坦克',
                SUM(CASE WHEN mount IN (10448, 10176, 10028, 10080, 10626) THEN 1 ELSE 0 END) AS '治疗'
            FROM team_all
        """
        )
        .df()
        .to_dict("records")[0]
    )

    result["leader_mount_type_count"]["all"] = {
        "item": list(d.keys()),
        "value": list(d.values()),
    }

    # each boss
    for boss_id in boss_lst:
        d = (
            duckdb.sql(
                f"""
                SELECT
                    SUM(CASE WHEN mount IN (10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756) THEN 1 ELSE 0 END) AS '外攻',
                    SUM(CASE WHEN mount IN (10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627) THEN 1 ELSE 0 END) AS '内攻',
                    SUM(CASE WHEN mount IN (10002, 10062, 10243, 10389) THEN 1 ELSE 0 END) AS '坦克',
                    SUM(CASE WHEN mount IN (10448, 10176, 10028, 10080, 10626) THEN 1 ELSE 0 END) AS '治疗'
                FROM team_each_boss
                WHERE achieve_id = {boss_id}
            """
            )
            .df()
            .to_dict("records")[0]
        )

        result["mount_type_attendance_count"][boss_id] = {
            "item": list(d.keys()),
            "value": list(d.values()),
        }

    ###################
    # 输出心法平均 DPS #
    ###################
    result["rank_mount_dps"] = {}

    # all
    result["rank_mount_dps"]["all"] = (
        duckdb.sql(
            """
            SELECT mount AS item, AVG(dps) AS value FROM raw
            WHERE mount IN (
                10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
            )
            GROUP BY mount
            ORDER BY AVG(dps) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["rank_mount_dps"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT mount AS item, AVG(dps) AS value FROM raw
                WHERE mount IN (
                    10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                    10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
                ) AND achieve_id = {boss_id}
                GROUP BY mount
                ORDER BY AVG(dps) DESC
            """
            )
            .df()
            .to_dict("list")
        )

    #####################
    # 输出心法平均伤害量 #
    #####################
    result["rank_mount_damage"] = {}

    # all
    result["rank_mount_damage"]["all"] = (
        duckdb.sql(
            """
            SELECT mount AS item, AVG(damage) AS value FROM raw
            WHERE mount IN (
                10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
            )
            GROUP BY mount
            ORDER BY AVG(damage) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["rank_mount_damage"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT mount AS item, AVG(damage) AS value FROM raw
                WHERE mount IN (
                    10026, 10015, 10144, 10145, 10224, 10268, 10390, 10464, 10533, 10585, 10698, 10756,
                    10003, 10021, 10175, 10014, 10081, 10225, 10242, 10447, 10615, 10627
                ) AND achieve_id = {boss_id}
                GROUP BY mount
                ORDER BY AVG(damage) DESC
            """
            )
            .df()
            .to_dict("list")
        )

    #####################
    # 治疗心法平均 HPS #
    #####################
    result["rank_mount_hps"] = {}

    # all
    result["rank_mount_hps"]["all"] = (
        duckdb.sql(
            """
            SELECT mount AS item, AVG(hps) AS value FROM raw
            WHERE mount IN (10002, 10062, 10243, 10389)
            GROUP BY mount
            ORDER BY AVG(damage) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["rank_mount_damage"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT mount AS item, AVG(hps) AS value FROM raw
                WHERE mount IN (10002, 10062, 10243, 10389) AND achieve_id = {boss_id}
                GROUP BY mount
                ORDER BY AVG(damage) DESC
            """
            )
            .df()
            .to_dict("list")
        )

    #####################
    # 治疗心法平均治疗量 #
    #####################
    result["rank_mount_therapy"] = {}

    # all
    result["rank_mount_therapy"]["all"] = (
        duckdb.sql(
            """
            SELECT mount AS item, AVG(therapy) AS value FROM raw
            WHERE mount IN (10002, 10062, 10243, 10389)
            GROUP BY mount
            ORDER BY AVG(damage) DESC
            """
        )
        .df()
        .to_dict("list")
    )

    # each boss
    for boss_id in boss_lst:
        result["rank_mount_therapy"][boss_id] = (
            duckdb.sql(
                f"""
                SELECT mount AS item, AVG(therapy) AS value FROM raw
                WHERE mount IN (10002, 10062, 10243, 10389) AND achieve_id = {boss_id}
                GROUP BY mount
                ORDER BY AVG(damage) DESC
            """
            )
            .df()
            .to_dict("list")
        )

    ########
    # dump #
    ########
    with open(output, "w+") as f:
        f.write(
            json.dumps(
                result, ensure_ascii=False, separators=(",", ":"), sort_keys=False
            )
        )


if __name__ == "__main__":
    parser = ArgumentParser()

    parser.add_argument("-b", "--boss", type=str, required=True)
    parser.add_argument("-i", "--input", type=str, required=True)
    parser.add_argument("-o", "--output", type=str, default="result.json")

    args = parser.parse_args()

    rank(args.input, args.output, args.boss)
