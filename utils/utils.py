import json


def remove_prefix(text: str, prefix: str) -> str:
    """Remove prefix from a string."""
    if text.startswith(prefix):
        return text[len(prefix) :]
    return text


def clean_json(elt):
    keys = list(elt.keys()).copy()
    for f in keys:
        if isinstance(elt[f], dict):
            elt[f] = clean_json(elt[f])
        elif (not elt[f] == elt[f]) or (elt[f] is None):
            del elt[f]
    return elt


def to_jsonl(input_list, output_file, mode="a"):
    with open(output_file, mode) as outfile:
        for entry in input_list:
            new = clean_json(entry)
            json.dump(new, outfile)
            outfile.write("\n")


def chunks(lst: list, n: int) -> list:
    """Yield n number of striped chunks from a list."""
    for i in range(0, n):
        yield lst[i::n]


def print_progress(count, total, steps=15) -> str:
    """Return a progress bar as a string."""
    completion = count / total
    done = int(completion * steps)
    remain = steps - done
    progress = f"[{'#' * (done)}{'.' * (remain)}] {count}/{total} ({completion:0.1%})"
    return progress
