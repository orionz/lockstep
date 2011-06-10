all: compile

compile: get-deps
	@./rebar compile

clean:
	@./rebar clean

get-deps:
	@./rebar get-deps

del-deps:
	@./rebar delete-deps
