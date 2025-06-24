import argparse
import pathlib

from generators.employees_generator import make_employees
from generators.job_history import make_job_history
from generators.comp_history import make_comp_history



def main():
    parser = argparse.ArgumentParser(
        description="Generate synthetic HR test data"
    )
    parser.add_argument("--rows", type=int, default=1000,
                        help="Number of employees to generate")
    parser.add_argument("--out", default="out",
                        help="Output directory for CSV files")
    args = parser.parse_args()

    out_dir = pathlib.Path(args.out)
    out_dir.mkdir(parents=True, exist_ok=True)

    # 1. Generate employee master data
    employees_df = make_employees(args.rows)
    employees_path = out_dir / "employees.csv"
    employees_df.to_csv(employees_path, index=False)
    print(f"✅ Wrote {len(employees_df)} employees → {employees_path}")

    # 2. Generate job history based on employees
    job_history_df = make_job_history(employees_df)
    job_history_path = out_dir / "job_history.csv"
    job_history_df.to_csv(job_history_path, index=False)
    print(f"✅ Wrote {len(job_history_df)} job records → {job_history_path}")

    # 3. Generate compensation history
    comp_history_df = make_comp_history(employees_df)
    comp_path = out_dir / "comp_history.csv"
    comp_history_df.to_csv(comp_path, index=False)
    print(f"✅ Wrote {len(comp_history_df)} comp rows → {comp_path}")



if __name__ == "__main__":
    main()
