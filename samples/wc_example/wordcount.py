def map_func(line: str):
    words = [w.strip() for w in line.lower().split() if w.strip()]
    return [(w, 1) for w in words]

def reduce_func(key: str, values: list[int]):
    # valores pueden venir como str si algún map los escribió así
    vv = [int(v) if isinstance(v, str) and v.isdigit() else v for v in values]
    return key, sum(vv)

# Opcional: si el archivo es pequeño y el master decide SINGLE, también funciona:
def process(input_path: str, output_path: str):
    counts = {}
    with open(input_path, "r", encoding="utf-8", errors="ignore") as f:
        for line in f:
            for w in line.lower().split():
                w = w.strip()
                if not w: continue
                counts[w] = counts.get(w, 0) + 1
    with open(output_path, "w", encoding="utf-8") as out:
        for k, v in counts.items():
            out.write(f"{k}\t{v}\n")
