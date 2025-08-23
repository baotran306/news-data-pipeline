from helpers.utils import parse_iso_datetime

class News:
    def __init__(self, id, title, url, summary, authors, source_country, publish_time):
        self.id = self.catch_number_value(id)
        self.title = self.format_text_string(title)
        self.url = self.format_text_string(url)
        self.summary = self.format_text_string(summary)
        self.source_country = self.format_text_string(source_country)
        self.authors = self.extract_list_authors(authors)
        self.publish_time = parse_iso_datetime(publish_time)

    @staticmethod
    def format_text_string(value):
        if value:
            # Remove redundant new lines, spaces
            lines = [
                line.strip()
                for line in value.split("\n")
                if line.strip() != ""
            ]
            return "\n".join(lines)
        else:
            return "Not Given"
    
    @staticmethod
    def catch_number_value(value):
        if isinstance(value, int) and value > 0:
            return value
        else:
            return -1
        
    @staticmethod
    def extract_list_authors(ls_authors) -> list:
        if ls_authors:
            if isinstance(ls_authors, list):
                return [author.strip() for author in ls_authors]
            else:
                # If not list, remove redundant space and treat as 1-len list
                return [str(ls_authors).strip()]
        else:
            return []
    
    def __str__(self):
        return f"""
- ID {self.id}: {self.title}
    + URL: {self.url}
    + Summary: {self.summary}
    + Source Country: {self.source_country}
    + Authors: {self.authors}
    + Publish time: {self.publish_time}
"""
    
    def as_dict(self):
        return {
            "id": self.id,
            "title": self.title,
            "url": self.url,
            "summary": self.summary,
            "source": self.source_country,
            "authors": self.authors,
            "publish_time": self.publish_time,
        }