namespace cpp demo
namespace java demo

struct Username {
  1: string firstName,
  2: string lastName,
}

service Doorman {
  string hello(1:string username),
  
  string helloEx(1:Username username)
}

