FILE = "data/sample_pyspark.py"


def main():
    with open(FILE, "r") as f:
        lines = f.readlines()
        for line in lines:
            if "dropDuplicates" in line:
                print(line)


if __name__ == "__main__":
    main()
