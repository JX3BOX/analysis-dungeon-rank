# 秘境百强榜 - 统计分析

## 统计指标
- `top10_achieve_team_count`: 前 10 个击杀 BOSS 的团队区服统计
- `top100_achieve_team_count`: 前 100 个击杀 BOSS 的团队区服统计
- `server_rank_team_count`: 入榜团队数量统计
- `force_attendance_count`: 门派出场统计
- `mount_attendance_count`: 心法出场统计
- `hps_count`: 治疗心法个数统计
- `hps_attendance_count`: 治疗心法出场统计
- `tank_count`: 防御心法个数统计
- `tank_attendance_count`: 防御心法出场统计
- `dps_count`: 输出心法统计
- `mount_type_attendance_count`: 内外功出场统计
- `leader_mount_type_count`: 团长心法类型统计
- `rank_mount_dps`: 输出心法平均 DPS
- `rank_mount_damage`: 输出心法平均伤害量
- `rank_mount_hps`: 治疗心法平均 HPS
- `rank_mount_therapy`: 治疗心法平均治疗量

## 统计结果数据结构
```json
{
    ":指标名称": {
        "all": {
            "item": ["全量已排序的统计指标内容"],
            "value": ["全量已排序的统计指标值"]
        },
        ":BOSS_ID":{
            "item": ["各个 BOSS 已排序的统计指标内容"],
            "value": ["各个 BOSS 已排序的统计指标值"]
        }
    }
}
```

## Quick Start
在运行前确保以下文件已处于工作目录:
- [https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/school.json](https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/school.json)
- [https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/mount_group.json](https://github.com/JX3BOX/jx3box-data/blob/master/data/xf/mount_group.json)

```bash
python DungeonRankAnalysis.py --input team_race_for_event.csv --output result.json --boss 11504,11501,11500,11502,11503
```
