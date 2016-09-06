
import hashlib

def checksum(content):
    """ SHA256+MD5
    196 characters
    """
    sha256 = hashlib.sha256()
    md5 = hashlib.md5()
    sha256.update(content.strip())
    md5.update(content.strip())
    return sha256.hexdigest().lower() + md5.hexdigest().lower()