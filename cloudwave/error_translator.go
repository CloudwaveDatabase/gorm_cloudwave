package cloudwave

import (
	"log"

	//"github.com/go-sql-driver/mysql"
	"github.com/cloudwavedatabase/database_sql_cloudwave"
)

var errCodes = map[string]uint16{
	"uniqueConstraint": 1062,
}

func (dialector Dialector) Translate(err error) error {
	if cloudwaveErr, ok := err.(*cloudwave.CloudWaveError); ok {
		//if mysqlErr.Number == errCodes["uniqueConstraint"] {  //weip ??????
		//if cloudwaveErr.briefMessage == "uniqueConstraint" {
		//	return gorm.ErrDuplicatedKey
		//}
		log.Println("...", cloudwaveErr)
	}

	return err
}
