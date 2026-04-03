import os
file_path = r"C:\Users\j_joh\OneDrive\Desktop\Visual_Studio\ems-data-project\data_raw\FACTPCRVITAL.txt"

print("\n=== NEMSIS DIAGNOSTIC ===")

# 1. Check file size ----------------------------------------

size_gb = os.path.getsize(file_path) / (1024**3)
print(f"File size: {size_gb:.2f} GB")

# 2. Read first few lines safely ------------------------------
print("\n--- First 5 lines (raw) ---")
with open(file_path, "r", errors="replace") as f:
    lines = [next(f).rstrip("\n") for _ in range(5)]
    for i, line in enumerate(lines):
        print(f"[Line {i+1}] {line[:300]}")   # print first 300 char only

# 3. Try to auto-detect delimiter ------------------------------
print("\n--- Delimiter detection ---")
possible_delims = [",", "\t", "|", ";", "~"]
first_line = lines[0]

delim_counts = {d: first_line.count(d) for d in possible_delims}
print("Delimiter counts:", delim_counts)

best_delim = max(delim_counts, key=delim_counts.get)
print(f"Most likely delimiter: '{best_delim}'")

# 4. Fixed-width check ----------------------------------------
print("\n--- Fixed width structure check ---")
lengths = [len(line) for line in lines]
print("Line lengths:", lengths)

if len(set(lengths)) == 1:
    print("✓ All sample lines same length → likely fixed-width file.")
else:
    print("✗ Lines differ in length → NOT fixed-width.")

# 5. Column preview using inferred delimiter --------------------
print("\n--- Column split preview ---")
split_cols = lines[0].split(best_delim)
print(f"Column count (first row): {len(split_cols)}")
print("First 10 columns:", split_cols[:10])

print("\n=== END OF DIAGNOSTIC ===\n")
