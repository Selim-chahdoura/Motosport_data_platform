import json

# open the file
with open("data_lake/raw/results/results_2023.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# level 1
races = data["Races"]
print("\nNumber of races:", len(races))

first_race = races[0]

print("\nFirst race keys:")
print(first_race.keys())

# level 2
results = first_race["Results"]
print("\nNumber of results in first race:", len(results))

first_result = results[0]

print("\nFirst result keys:")
print(first_result.keys())

# level 3
print("\nDriver keys:")
print(first_result["Driver"].keys())

print("\nConstructor keys:")
print(first_result["Constructor"].keys())