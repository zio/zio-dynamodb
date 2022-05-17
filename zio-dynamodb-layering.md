## Layering recommendations

- functional data modelling
    - library assumes you use FDM principles - ie use a combination of products (case classes) and sum types/coproducts (sealed traits)
    - library allows you to select sum type codec strategies through annotations to enable compatibility with existing schema's
- layering
    - DB layer
        - AttrMap/DB specific model [area of concern for this library]
            - so I don't think we do not need infinite extensibility to handle ALL modelling requirements eg newtypes etc
            - advantage is that we surface DDB model explicitly in terms of Scala - this step is often missing
                - we often go from Core model to DDB AttrVals with manual transformations that are not immediately visible
                - using this new approach we reify the DynamoDB model 1:1 ie make it concrete and visible as a Scala model
    - service layers
        - Core model - higher degrees of extensibility required here eg new types, enrichment of data
    - adapters/outward facing modules
        - interface specific model - higher degrees of extensibility/flexibility required here

- testing for integrating with existing DBs
    - Note AttMap represents the "truth" in the DB so create a reference AttrMap Item first
    - create a model using FDM principles that will return the AttrMap shape that you want, using annotations to specify the strategy
      for your sum types. The model can be tested using the steps below:
        - use the In memory fake DB that stores AttrMap values to verify the type safe model by writing the below tests:
        - Test 1 - use low level API to write the reference AttrMap to DB and read using high level API + Model type
        - Test 2 - write using high level API + Model and read using low level API to verify against reference AttrMap

- Versioning
    - If versioning of data is employed then Top level Polymorphic Sum types could be used to safely model different version of DB
      held concurrently eg `InvoiceV1`, `InvoiceV2` 


