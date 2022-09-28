from app.actions import dedupe_employer_records

if __name__ == "__main__":
    i = None

    while i != "q":
        prompt = (
            "What would you like to do?\n"
            "(t)rain model or add to existing dataset\n"
            "(r)eview clusters (10 at a time)\n"
            "(g)enerate unique employers from clustered records\n"
            "(q)uit\n"
        )
        print(prompt)
        i = input("Enter action: ")
        valid_responses = ["t", "r", "g", "q"]

        while i not in valid_responses:
            print(prompt)
            i = input("Enter action: ")

        if i == "t":
            dedupe_employer_records.interactively_train_model()
            print("Finished training, building cluster table...")
            dedupe_employer_records.build_cluster_table()

        if i == "r":
            dedupe_employer_records.review_clusters(10)

        if i == "g":
            dedupe_employer_records.generate_canonical_employers_from_clusters()
