const sidebars = {
  sidebar: [
    {
      type: "category",
      label: "ZIO DynamoDB",
      collapsed: false,
      link: { type: "doc", id: "index" },
      items: [
        // Concepts
        {
          type: "category",
          collapsed: false,
          label: "Concepts",
          items: [
            "concepts/architecture",
            "concepts/high-level-api",
            "concepts/low-level-api",
            "concepts/transactions",
          ],

        },
        // Guides
        {
          type: "category",
          collapsed: false,
          label: "Guides",
          items: [
            "guides/getting-started",
            "guides/cheat-sheet",
            "guides/codec-customization"
          ]
        },
        // Reference
        {
          type: "category",
          collapsed: false,
          label: "Reference",
          items: [
            {
              type: "category",
              label: "High Level API",
              collapsed: true,
              link: { type: "doc", id: "reference/hi-level-api/index" },
              items: [
                "reference/hi-level-api/item1",
              ]
            },
            {
              type: "category",
              label: "Low Level API",
              collapsed: true,
              link: { type: "doc", id: "reference/low-level-api/index" },
              items: [
                "reference/low-level-api/attribute-value",
              ]
            },
            "reference/dynamodb-query",
            "reference/projection-expression",
            "reference/primary-keys",
          ]
        }
      ]
    }
  ]
};

/*
            {
              type: "category",
              label: "Codecs",
              collapsed: true,
              link: { type: "doc", id: "derivations/codecs/index" },
              items: [
                "derivations/codecs/avro",
                "derivations/codecs/thrift",
                "derivations/codecs/bson",
                "derivations/codecs/json",
                "derivations/codecs/message-pack",
                "derivations/codecs/protobuf",
              ],
            },
*/

module.exports = sidebars;