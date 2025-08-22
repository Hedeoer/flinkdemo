
借助项目https://github.com/MaterializeInc/datagen.git生成模拟数据

blog.json

```json
[
    {
        "_meta": {
            "topic": "users",
            "key": "id",
            "relationships": [
                {
                    "topic": "posts",
                    "parent_field": "id",
                    "child_field": "user_id",
                    "records_per": 2
                }
            ]
        },
        "id": "faker.datatype.number(100)",
        "name": "faker.internet.userName()",
        "email": "faker.internet.exampleEmail()",
        "phone": "faker.phone.imei()",
        "website": "faker.internet.domainName()",
        "city": "faker.address.city()",
        "company": "faker.company.name()"
    },
    {
        "_meta": {
            "topic": "posts",
            "key": "id",
            "relationships": [
                {
                    "topic": "comments",
                    "parent_field": "id",
                    "child_field": "post_id",
                    "records_per": 2
                }
            ]
        },
        "id": "faker.datatype.number(1000)",
        "user_id": "faker.datatype.number(100)",
        "title": "faker.lorem.sentence()",
        "body": "faker.lorem.paragraph()"
    },
    {
        "_meta": {
            "topic": "comments",
            "key": "id",
            "relationships": [
                {
                    "topic": "users",
                    "parent_field": "user_id",
                    "child_field": "id",
                    "records_per": 1
                }
            ]
        },
        "id": "faker.datatype.number(2000)",
        "user_id": "faker.datatype.number(100)",
        "body": "faker.lorem.paragraph()",
        "post_id": "faker.datatype.number(1000)",
        "views": "faker.datatype.number({min: 100, max: 1000})",
        "status": "faker.datatype.number(1)"
    }
]


```
输出生成到kafak
```shell
datagen `
    --schema ./blog.json `
    --prefix mz_datagen_blog `
    --number -1 `
    --wait 1000
```


ecommerce.json
```json
[
  {
    "_meta": {
      "topic": "users",
      "key": "id",
      "relationships": [
        {
          "topic": "purchases",
          "parent_field": "id",
          "child_field": "user_id",
          "records_per": 4
        }
      ]
    },
    "id": "faker.datatype.number(1000)",
    "name": "faker.internet.userName()",
    "email": "faker.internet.exampleEmail()",
    "city": "faker.address.city()",
    "state": "faker.address.state()",
    "zipcode": "faker.address.zipCode()"
  },
  {
    "_meta": {
      "topic": "purchases",
      "key": "id",
      "relationships": [
        {
          "topic": "items",
          "parent_field": "item_ids",
          "child_field": "id"
        }
      ]
    },
    "id": "faker.datatype.uuid()",
    "user_id": "this string can be anything since this field is determined by user.id",
    "item_ids": "faker.helpers.uniqueArray((()=>{return Math.floor(Math.random()*5000);}), Math.floor(Math.random()*4+1))",
    "total": "faker.commerce.price(25, 2500)",
    "order_time": "faker.date.recent(1)"
  },
  {
    "_meta": {
      "topic": "items",
      "key": "id"
    },
    "id": "this string can be anything since this field is determined by purchases.item_ids",
    "name": "faker.commerce.product()",
    "price": "faker.commerce.price(5, 500)",
    "description": "faker.commerce.productDescription()",
    "material": "faker.commerce.productMaterial()"
  }
]
```

生成数据到kafka
```shell
datagen `
    --schema ./ecommerce.json `
    --prefix mz_datagen_ecommerce `
    --number -1 `
    --wait 1000
```