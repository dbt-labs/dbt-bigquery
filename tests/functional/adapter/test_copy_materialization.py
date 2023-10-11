import pytest
from pathlib import Path
from dbt.tests.util import run_dbt, write_file, check_relations_equal

_SEED_A = """
load_date,id,first_name,last_name,email,gender,ip_address
2021-03-05,1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2021-03-05,2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
2021-03-05,3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
""".lstrip()

_SEED_B = """
load_date,id,first_name,last_name,email,gender,ip_address
2021-03-05,4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
2021-03-05,5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
""".lstrip()

_EXPECTED_RESULT = """
load_date,id,first_name,last_name,email,gender,ip_address
2021-03-05,1,Jack,Hunter,jhunter0@pbs.org,Male,59.80.20.168
2021-03-05,2,Kathryn,Walker,kwalker1@ezinearticles.com,Female,194.121.179.35
2021-03-05,3,Gerald,Ryan,gryan2@com.com,Male,11.3.212.243
2021-03-05,4,Bonnie,Spencer,bspencer3@ameblo.jp,Female,216.32.196.175
2021-03-05,5,Harold,Taylor,htaylor4@people.com.cn,Male,253.10.246.136
""".lstrip()

_COPY_MODEL = """
{{ config(
    materialized="copy",
    copy_materialization="incremental",
) }}

SELECT * FROM {{ ref("seed") }}
"""


class BaseCopyModelConfig:
    @pytest.fixture(scope="class")
    def models(self):
        return {"copy_model.sql": _COPY_MODEL}

    @pytest.fixture(scope="class")
    def seeds(self):
        return {
            "seed.csv": _SEED_A,
            "expected_result.csv": _EXPECTED_RESULT,
        }


class TestCopyMaterialization(BaseCopyModelConfig):
    def test_incremental_copy(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        # Replace original seed _SEED_A with _SEED_B
        seed_file = project.project_root / Path("seeds") / Path("seed.csv")
        write_file(_SEED_B, seed_file)

        run_dbt(["seed"])
        run_dbt(["run"])

        check_relations_equal(project.adapter, ["copy_model", "expected_result"])
