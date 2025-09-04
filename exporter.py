class Exporter:
    def export(self, headers, data, path):
        raise NotImplementedError

class CSVExporter(Exporter):
    def export(self, headers, data, path):
        import csv
        with open(path, "w", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(data)

# TODO: implement JSON exporter