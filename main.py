import os
import xml.etree.ElementTree as et
from ftplib import FTP
import gzip
import tarfile
import json
import chromadb
from chromadb import Settings
import chromadb.utils.embedding_functions as embedding_functions


def extract_texts2(filename: str, storage_loc: str) -> dict:
    document_dict = {}

    try:
        with tarfile.open(name=storage_loc + "/" + filename, mode="r:gz") as tar:
            for member in tar.getmembers():
                print(member.name)
                f = tar.extractfile(member)
                if f is not None:
                    content = f.read()
                    print(len(content))
                    try:
                        json_contents = json.loads(content)

                        for documents in json_contents['documents']:
                            doc_id = documents['id']
                            document_list = []
                            for passages in documents['passages']:
                                document_list.append(passages['text'])
                                if len(''.join(document_list)) > 0:
                                    document_dict[doc_id] = ''.join(document_list)
                    except Exception as e:
                        print(e)
    except Exception as e:
        print(e)

    return document_dict


def extract_texts(filename: str, storage_loc: str) -> dict:
    document_dict = {}

    try:
        with gzip.open(filename=storage_loc+"/"+filename, mode='rb') as f:
            file_content = f.read()
            print("File Content Size:", len(file_content))
            with tarfile.open(file_content) as tar:
                members = tar.getmembers()
                print(len(members))
                for member in members:
                    if member.isfile():
                        if member.name.endswith(".xml"):
                            print("Working:", member.name)
                            try:
                                contents = tar.extractfile(member).read()
                                json_contents = json.loads(contents)

                                for documents in json_contents['documents']:
                                    doc_id = documents['id']
                                    document_list = []
                                    for passages in documents['passages']:
                                        document_list.append(passages['text'])
                                if len(''.join(document_list)) > 0:
                                    document_dict[doc_id] = ''.join(document_list)
                            except Exception as e:
                                print(e)
    except Exception as e:
        print(e)

    return document_dict


def extract_abstracts(filename: str, storage_loc: str) -> dict:
    abstracts = {}

    try:
        with gzip.open(filename=storage_loc+"/"+filename, mode='rb') as f:
            file_content = f.read()
            doc = et.fromstring(file_content)

            for articles in doc:
                for article in articles:
                    if article.tag == 'MedlineCitation':
                        article_pmid = article.find("PMID").text
                        if article.find("Article/Abstract/AbstractText") is not None:
                            texts = article.find("Article/Abstract/AbstractText").text
                            if len(texts) > 0:
                                abstracts[article_pmid] = texts

    except Exception as e:
        print(e)

    return abstracts


def get_abstracts(baseline_loc: str, storage_loc: str, ftp_conn: FTP) -> None:
    pad_width = 4
    ftp_conn.cwd(dirname=baseline_loc)
    for i in range(1, 1220):
        num = str(i).zfill(pad_width)
        filename = "pubmed24n" + num + ".xml.gz"
        with open(storage_loc + "/" + filename, 'wb') as fp:
            ftp_conn.retrbinary(f"RETR {filename}", fp.write)


def ftp_client(ftp_loc: str) -> FTP:
    ftp = FTP(ftp_loc)
    ftp.login()
    return ftp


def chroma_client(path: str = "/data/vector/chromadb", settings: Settings = Settings()):
    return chromadb.PersistentClient(path=path, settings=settings)


def main():
    storage_loc = "/data"
    client = chroma_client()
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(model_name="sentence-transformers/multi-qa-mpnet-base-cos-v1", device="cuda") 

    # Get abstracts from ftp
    # baseline_loc = "pubmed/baseline"
    # ftp_loc = "ftp.ncbi.nlm.nih.gov"
    # ftp = ftp_client(ftp_loc)
    # get_abstracts(baseline_loc=baseline_loc, storage_loc=storage_loc, ftp_conn=ftp)

    # Add abstracts
    #
    # collection = client.get_collection(name="pubmed_abstracts", embedding_function=ef)
    # abstract_loc = storage_loc + "/PubMed/Abstracts"
    # for filename in os.listdir(abstract_loc):
        # f = os.path.join(abstract_loc, filename)
        # if os.path.isfile(f) and filename.endswith(".gz"):
            # print(filename)
            # abstracts = extract_abstracts(filename=filename, storage_loc=abstract_loc)
            # print(len(abstracts))
            # filelist = [{"filename": filename}]
            # filelist = filelist * len(abstracts)
            # try:
                # collection.upsert(documents=list(abstracts.values()), ids=list(abstracts.keys()), metadatas=filelist)
            # except Exception as e:
                # print(e)
                # pass

    # Get texts from ftp
    # baseline_loc = "/pub/wilbur/BioC-PMC"
    # ftp = ftp_client(ftp_loc)

    # Add texts
    collection = client.get_or_create_collection(name="pubmed_texts", embedding_function=ef)
    text_loc = storage_loc + "/PubMed/FullText"

    for filename in os.listdir(text_loc):
        f = os.path.join(text_loc, filename)
        if os.path.isfile(f) and filename.endswith(".gz"):
            print("Extracting:", filename)
            texts = extract_texts2(filename=filename, storage_loc=text_loc)
            print("Found", len(texts), "items")
            filelist = [{"filename": filename}]
            filelist = filelist * len(texts)
            for k, v in texts.items():
                try:
                    print("Trying upsert", k)
                    collection.upsert(documents=list(v), ids=list(k), metadatas=filelist)
                    # collection.add(documents=[v], ids=[k], metadatas=[{"filename": filename}])
                except Exception as e:
                    print(e)
                    # pass


if __name__ == '__main__':
    pattern = "pubmed24n0001.xml.gz"
    main()

