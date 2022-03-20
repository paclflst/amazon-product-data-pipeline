from requests.models import Response
import os

class FileService:
    pass

    def save_file_from_response(self, file_name: str, r: Response, batch_sile=1024*5):
        os.makedirs(os.path.dirname(file_name), exist_ok=True)
        with open(file_name, 'wb') as f:
            for part in r.iter_content(batch_sile):
                f.write(part)
        return True

    
    def merge_files_into_one(self, file_name: str, file_list: [str]):
        with open(file_name, 'wb') as o:
            for chunk_path in file_list:
                with open(chunk_path, 'rb') as s:
                    o.write(s.read())

                os.remove(chunk_path)
        return True
