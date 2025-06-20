#!/usr/bin/env python
"""Generate a synthetic employees.csv file (Norwegian locale by default)."""

from faker import Faker          # ✅ keep only Faker here
import random                    # ✅ Python’s std-lib RNG

import argparse
import datetime as dt
import pathlib
import pandas as pd


fake = Faker("no_NO")


def make_employees(rows: int = 1_000, seed: int = 42) -> pd.DataFrame:
    """Return a DataFrame with <rows> synthetic employee records."""
    Faker.seed(seed)
    random.seed(seed)

    recs = []
    for i in range(rows):
        gender = random.choice(["F", "M"])
        first  = fake.first_name_female() if gender == "F" else fake.first_name_male()
        last   = fake.last_name()
        dob    = fake.date_of_birth(minimum_age=18, maximum_age=65)   # explicit args

        recs.append(
            {
                "EmployeeID": f"E{i+1:05d}",
                "FirstName":  first,
                "LastName":   last,
                "Gender":     gender,
                "DOB":        dob.isoformat(),
                "Age":        dt.date.today().year - dob.year,
                "SSN":        fake.ssn(),         # Norwegian-style fødselsnr
            }
        )
    return pd.DataFrame(recs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Generate synthetic employees CSV (HR test data)"
    )
    parser.add_argument("--rows", type=int, default=1_000,
                        help="number of employee rows to generate")
    parser.add_argument("--out", default="out",
                        help="output folder (will be created if needed)")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    csv_path = out_dir / "employees.csv"
    make_employees(args.rows).to_csv(csv_path, index=False)
    print(f"✅ Wrote {args.rows:,} rows → {csv_path}")
