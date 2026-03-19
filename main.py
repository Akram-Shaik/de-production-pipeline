import pandas as pd
import sqlite3

def clean_name(full_name):
    # This function takes "John Albert Doe" and returns "John Doe"
    parts = str(full_name).split()
    if len(parts) >= 2:
        return f"{parts[0]} {parts[-1]}"
    return full_name

def run_my_first_pipeline():
    print("🚀 Pipeline Started...")

    # 1. EXTRACT: Read the CSV file
    df = pd.read_csv('data.csv')
    print("✅ Data extracted from CSV.")

    # 2. TRANSFORM (CLEAN IMMEDIATELY)
    # Apply our custom clean_name function to every row
    df['name'] = df['name'].apply(clean_name)
    df['name'] = df['name'].str.title().str.strip()

    # Fill missing salaries with the average salary so the math doesn't break
    df['salary'] = df['salary'].fillna(df['salary'].mean())

    # 3.Filter (Using the now-clean data) and give them a 10% raise
    # This is a classic "Data Engineering" task
    eng_staff = df[df['department'] == 'Engineering'].copy()
    eng_staff['salary'] = eng_staff['salary'] * 1.20
    print("✅ Transformation complete: Engineering salaries increased by 20%.")

    # 4. LOAD: Save this into a SQL Database
    # This creates a file called 'company.db' automatically
    conn = sqlite3.connect('company.db')
    eng_staff.to_sql('engineering_promotions', conn, if_exists='replace', index=False)
    print("✅ Data loaded into SQL (Engineering_promotions table).")

    # 5. VERIFY: Query the SQL database to prove it worked
    print("\n--- Final SQL Report (High Earners) ---")
    report = pd.read_sql_query("SELECT name, salary FROM engineering_promotions WHERE salary > 80000", conn)
    print(report)

    # Strip extra spaces from names
    df['name'] = df['name'].str.strip().str.title() 
  
    
    #Second piece of code starts here group by()
    dept_summary = df.groupby('department')['salary'].sum().reset_index()

    # 2. LOAD: Save this summary into a NEW SQL table
    dept_summary.to_sql('department_stats', conn, if_exists='replace', index=False)

    print("✅ Department summary created and loaded to SQL.")

    # 3. VERIFY: Query the new table
    print("\n--- Department Salary Spend ---")
    stats_report = pd.read_sql_query("SELECT * FROM department_stats", conn)
    print(stats_report)


    conn.close()
    print("\n🏆 Pipeline finished successfully!")

if __name__ == "__main__":
    run_my_first_pipeline()