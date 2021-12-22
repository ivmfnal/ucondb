import hashlib, uuid

def calculate_signature(folder, user, password, salt, data, algorithm = "sha512"):
        assert algorithm in hashlib.algorithms_available
        m = hashlib.new(algorithm)
        m.update(self.Folder)
        m.update(":")
        m.update(self.User)
        m.update(":")
        m.update(self.Password)
        m.update(":")
        m.update(self.Salt)
        m.update(":")
        m.update(self.Data)
        return algorithm + ":" + m.hexdigest()

def generate_signature(folder, user, password, data):
    alg = "sha512"
    assert alg in hashlib.algorithms_available
    salt = uuid.uuid1().hex
    return calculate_signature(folder, user, password, salt, data, alg)

def verify_signature(signature, folder, user, password, salt, data):
    alg, sig = signature.split(":", 1)
    calculated = calculate_signature(folder, user, password, salt, data, alg)
    return calculated == signature
    
class Signature:

    def __init__(self, folder, data):
        self.Folder = folder
        self.Data = data
        
    def calculate(self, user, password, salt, algorithm):
        assert algorithm in hashlib.algorithms_available
        assert not (":" in salt)
        m = hashlib.new(algorithm)
        m.update(self.Folder)
        m.update(":")
        m.update(user)
        m.update(":")
        m.update(password)
        m.update(":")
        m.update(salt)
        m.update(":")
        m.update(self.Data)
        return user + ":" + algorithm + ":" + salt + ":" + m.hexdigest()
        
    def generate(self, user, password):
        alg = "sha512"
        assert alg in hashlib.algorithms_available
        salt = uuid.uuid1().hex
        return self.calculate(user, password, salt, alg)
        
    def verify(self, signature, get_password):
        user, alg, salt, sig = signature.split(":", 3)
        password = get_password(user)
        if password is None:    return False
        calculated = self.calculate(user, password, salt, alg)
        return calculated == signature
        
