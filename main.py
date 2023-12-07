from tasks.tasks import GetReports

def main():
    # Create an instance of GetReports
    get_reports_instance = GetReports()

    df_merged = get_reports_instance.NADAC_mapping()
    df_filtered_orange_book = get_reports_instance.orange_book_retrieive()
    df_fears = get_reports_instance.parse_the_FEARS(df_filtered_orange_book)

    print(df_merged.head())
    print(df_filtered_orange_book.head())
    print(df_fears.head())


if __name__ == "__main__":
    main()
