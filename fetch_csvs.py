#!/usr/bin/env python3


def main():
    args = get_args()
    filtr = FILTERS[args.filter_name](**vars(args))
    csv_files = {}
    for table_name in filtr.get_filtered_data():
        table_data = filtr.merge_filtered_data(filtr.get_filtered_data(), table_name)
        csv_files[table_name] = write_csv(table_data, table_name, **vars(args))

    from sendEmail import BaseHTMLClass, send_email
    table_files = [csv_files[name] for name in ["Users", "Projects", "Schedds"]]
    html = BaseHTMLClass(table_files).get_html()
    send_email(
        from_addr="jpatton@cs.wisc.edu",
        to=["jpatton@cs.wisc.edu"],
        subject="Test OSG Usage Report",
        html=html,
        table_files=table_files)
    

if __name__ == "__main__":
    main()
