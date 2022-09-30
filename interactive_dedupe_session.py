from app.actions.dedupe import (
    build_cluster_table,
    generate_canonical_employers_from_clusters,
    review_clusters,
    train_dedupe_model,
)

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
            print(
                "Analyzing records to generate training set (this may take a while)..."
            )
            train_dedupe_model.train_dedupe_model()
            print("Finished training, building cluster table...")
            build_cluster_table.build_cluster_table()

        if i == "r":
            review_clusters.review_clusters(10)

        if i == "g":
            generate_canonical_employers_from_clusters.generate_canonical_employers_from_clusters()
