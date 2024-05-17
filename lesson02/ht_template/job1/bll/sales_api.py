from lesson02.ht_template.job1.dal import local_disk, sales_api


def save_sales_to_local_disk(date: str, raw_dir: str) -> None:
    """It retrieves sales data from API and writes it to file in provided directory."""
    # get data from the API
    sales_data = sales_api.get_sales(date)
    # save data to disk
    filename: str = f'{date}.json'
    local_disk.save_to_disk(sales_data, path=raw_dir, filename=filename)
