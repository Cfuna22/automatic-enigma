def deco(func):
    def wrapper():
        print('starting')
        func()
        print('done')
    return wrapper

@deco
def hello():
    print('hello')

hello()
