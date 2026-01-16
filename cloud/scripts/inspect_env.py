from pathlib import Path

# Path to your .env file
env_path = Path(__file__).resolve().parent.parent / '.env'

print(f"--- Inspecting file: {env_path} ---")

try:
    with open(env_path, 'r') as f:
        lines = f.readlines()
        for i, line in enumerate(lines):
            # Strip whitespace and check if it's an empty line or comment
            clean_line = line.strip()
            if not clean_line or clean_line.startswith('#'):
                continue
                
            # Split by '=' to see the variable name
            if '=' in clean_line:
                var_name = clean_line.split('=')[0].strip()
                print(f"Line {i+1}: Found variable named '{var_name}'")
            else:
                print(f"Line {i+1}: Potential formatting error (no '=' found): '{clean_line}'")
except Exception as e:
    print(f"❌ Could not read file: {e}")