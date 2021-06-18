FROM gcc:latest

COPY arg_printer.cc arg_printer.cc
RUN g++ -o arg_printer arg_printer.cc

ENTRYPOINT ["./arg_printer"]
