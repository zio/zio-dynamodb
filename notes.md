# Streaming/Paging problem 


# TODO
- Misc
    - Home brew MapOfSet  
- Query model
    - [X] Initial Query model design
    - [X] Bottom out Streaming/Paging interfaces
    - add missing popular Constructors
        - [ ] transaction support
            - there are Transaction based equivalents of BatchWriteItem/BatchReadItem that also support `updates`
        - [ ] DescribeTable, ListTables, can't think of any more popular ones for initial version of lib ATM
    - [ ] more unit tests
- DSL
    - [ ] initial design
- get project build working
    - [ ] use same pattern as for `zio-akka`
- docs
    - [ ] maybe get this TODO list into the docs?
- [ ] protocol for calling DDB - suggested options include
    - AWS java SDK
    - zio-aws?
        - pros - ZIO/Scala integration, simple/quicker to use
        - cons
            - extra dependency forced on user
            - performance ???
    - direct REST API access
        - cons
            - biggish piece of work
            - will need to keep in step with AWS API changes
        - pros - addresses all the cons of zio-aws
    - bottom out AWS config for User
- [ ] implement realistic DDB fake - some suggestions:
    - create own in memory Fake?
        - a considerable amount of work as many features to implement
    - AWS DDB local JAR
        - lighter weight to use than local stack (provides an in-memory DB without the need for docker) and has best DDB feature support
        - suitable for finer grained unit tests
    - localstack
        - do we even need this?
        - maybe have this for simple Integration tests that prove AWS connectivity
- [ ] return meta data for mutation queries
    - this is a reasonably sized job as there are a lot of mutation Constructors

# DONE

- go through BATCH partial results/partial failure scenarios with John
    - first release we could fail on any failure
    - on partial failure we could return unsuccessful items in the error channel
    - next we could retry up to N times in DDBExecutor live implementation
