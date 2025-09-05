import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from pathlib import Path
import os


def load_data(file_path):
    """
    Load data from a CSV file into a pandas DataFrame.
    """
    try:
        data = pd.read_csv(file_path, sep="\t")
        return data
    except FileNotFoundError:
        print(f"Error: The file {file_path} was not found.")
        return pd.DataFrame()
    except pd.errors.EmptyDataError:
        print("Error: The file is empty.")
        return pd.DataFrame()
    except Exception as e:
        print(f"An error occurred while loading the data: {e}")
        return pd.DataFrame()


def count_errors(df: pd.DataFrame):
    """
    Calculate the percentage of each error type in the DataFrame.
    """
    errors = df["error"].fillna("None").value_counts().reset_index(name="count")
    total_errors = sum(errors["count"])
    errors_percent = errors.copy()
    errors_percent["percent"] = errors_percent["count"] / total_errors
    return errors_percent


def count_subpages(df: pd.DataFrame):
    """
    Count the number of subpages in the DataFrame.
    """
    subpages = df.groupby("dl_slot").size().reset_index(name="count")
    subpages["count"] = subpages["count"] - 1
    # print(subpages.groupby("count").size())
    subpages.groupby("count").size().reset_index("count").plot(kind="bar", legend=False)
    # plt.show()
    return subpages


def summary_statistics(df: pd.DataFrame, log_path="logs/runs.csv"):
    timestamp = datetime.now().isoformat()
    errors = count_errors(df).set_index("error")
    subpages = count_subpages(df).groupby("count").size().reset_index(name="subpages")
    print(subpages)

    error_record = []
    subpage_record = []

    for error in errors.index:
        error_record.append(
            {
                "timestamp": timestamp,
                "error_type": error,
                "count": int(errors.loc[error]["count"]),
                "percent": float(errors.loc[error]["percent"]),
            }
        )
    error_record = pd.DataFrame(error_record)

    for subpage in subpages.index:
        subpage_record.append(
            {
                "timestamp": timestamp,
                "subpage": int(subpages.loc[subpage]["count"]),
                "count": int(subpages.loc[subpage]["subpages"]),
            }
        )
    subpage_record = pd.DataFrame(subpage_record)

    os.makedirs(os.path.dirname("logs/errors.csv"), exist_ok=True)
    if os.path.exists("logs/errors.csv"):
        error_record.to_csv("logs/errors.csv", mode="a", header=False, index=False)
    else:
        error_record.to_csv("logs/errors.csv", mode="w", header=True, index=False)

    os.makedirs(os.path.dirname("logs/subpages.csv"), exist_ok=True)
    if os.path.exists("logs/subpages.csv"):
        subpage_record.to_csv("logs/subpages.csv", mode="a", header=False, index=False)
    else:
        subpage_record.to_csv("logs/subpages.csv", mode="w", header=True, index=False)


def save_subpage_histogram(df, outdir="plots/"):
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    outfile = os.path.join(outdir, f"hist_subpages_{timestamp}.png")
    os.makedirs(outdir, exist_ok=True)

    # count_subpages(data).groupby("count").size().reset_index(
    # name="subpages").set_index("count").plot(kind="bar")

    df.groupby("dl_slot").size().reset_index(name="count").plot(kind="hist")

    plt.title("Distribution of subpages per company")
    plt.xlabel("Number of subpages")
    plt.ylabel("Number of companies")
    plt.tight_layout()
    plt.savefig(outfile)
    plt.close()


def merge_files():
    chunks_dir = Path(__file__).resolve().parent.parent / "chunks"
    output_file = chunks_dir / "ARGUS_merged_output.csv"

    # Find all files matching the pattern
    chunk_files = sorted(
        [f for f in chunks_dir.glob("ARGUS_chunk_p*.csv") if f.is_file()]
    )

    # Read and concatenate all chunk files
    merged_df = pd.concat(
        (pd.read_csv(f, sep="\t", encoding="utf-8") for f in chunk_files),
        ignore_index=True,
    )

    # Save the merged result
    merged_df.to_csv(output_file, sep="\t", index=False, encoding="utf-8")

    print(f"Merged {len(chunk_files)} files into {output_file}")


# data = load_data(
#     "/Users/mattiasalvetti/Desktop/KOF/python/ARGUS/chunks/ARGUS_merged_output.csv")
# print(data.groupby("dl_slot").size().reset_index(name="count").plot(kind="hist"))
# plt.show()

root = Path(__file__).resolve().parents[2]
csv = root / "url_panel_10k.csv"
# data = pd.read_csv(
# "", delimiter=",", index_col="url", on_bad_lines="skip", engine="python"
# )
# data = data.sample(n=100)
# print(data)
# data.to_csv("100subset.csv", sep=",", index=True, encoding="utf-8")
