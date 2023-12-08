package cloudwave

import (
	"database/sql"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/migrator"
	"gorm.io/gorm/schema"
)

const indexSql = `
SELECT
	TABLE_NAME,
	COLUMN_NAME,
	INDEX_NAME,
	NON_UNIQUE 
FROM
	information_schema.STATISTICS 
WHERE
	TABLE_SCHEMA = ? 
	AND TABLE_NAME = ? 
ORDER BY
	INDEX_NAME,
	SEQ_IN_INDEX`

var typeAliasMap = map[string][]string{
	"bool":    {"tinyint"},
	"tinyint": {"bool"},
}

type Migrator struct {
	migrator.Migrator
	Dialector
}

func (m Migrator) FullDataTypeOf(field *schema.Field) clause.Expr {
	expr := m.Migrator.FullDataTypeOf(field)

	if value, ok := field.TagSettings["COMMENT"]; ok {
		expr.SQL += " COMMENT " + m.Dialector.Explain("?", value)
	}

	return expr
}

func (m Migrator) AlterColumn(value interface{}, field string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if stmt.Schema != nil {
			if field := stmt.Schema.LookUpField(field); field != nil {
				fullDataType := m.FullDataTypeOf(field)
				if m.Dialector.DontSupportRenameColumnUnique {
					fullDataType.SQL = strings.Replace(fullDataType.SQL, " UNIQUE ", " ", 1)
				}

				return m.DB.Exec(
					"ALTER TABLE ? MODIFY COLUMN ? ?",
					clause.Table{Name: stmt.Table}, clause.Column{Name: field.DBName}, fullDataType,
				).Error
				//weip ???????

				sql := "ALTER TABLE " + stmt.Table + " MODIFY COLUMN " + field.DBName + " bigint"
				return m.DB.Exec(sql).Error
			}
		}
		return fmt.Errorf("failed to look up field with name: %s", field)
	})
}

func (m Migrator) AddColumn(value interface{}, name string) error {
	if m.DontSupportAddColumn {
		return nil
	}
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		// avoid using the same name field
		f := stmt.Schema.LookUpField(name)
		if f == nil {
			return fmt.Errorf("failed to look up field with name: %s", name)
		}

		if !f.IgnoreMigration {
			return m.DB.Exec(
				"ALTER TABLE ? ADD ? ?",
				m.CurrentTable(stmt), clause.Column{Name: f.DBName}, m.DB.Migrator().FullDataTypeOf(f),
			).Error
			//weip ???????
		}

		return nil
	})
}

func (m Migrator) TiDBVersion() (isTiDB bool, major, minor, patch int, err error) {
	// TiDB version string looks like:
	// "5.7.25-TiDB-v6.5.0" or "5.7.25-TiDB-v6.4.0-serverless"
	tidbVersionArray := strings.Split(m.Dialector.ServerVersion, "-")
	if len(tidbVersionArray) < 3 || tidbVersionArray[1] != "TiDB" {
		// It isn't TiDB
		return
	}

	rawVersion := strings.TrimPrefix(tidbVersionArray[2], "v")
	realVersionArray := strings.Split(rawVersion, ".")
	if major, err = strconv.Atoi(realVersionArray[0]); err != nil {
		err = fmt.Errorf("failed to parse the version of TiDB, the major version is: %s", realVersionArray[0])
		return
	}

	if minor, err = strconv.Atoi(realVersionArray[1]); err != nil {
		err = fmt.Errorf("failed to parse the version of TiDB, the minor version is: %s", realVersionArray[0])
		return
	}

	if patch, err = strconv.Atoi(realVersionArray[2]); err != nil {
		err = fmt.Errorf("failed to parse the version of TiDB, the patch version is: %s", realVersionArray[0])
		return
	}

	isTiDB = true
	return
}

func (m Migrator) RenameColumn(value interface{}, oldName, newName string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		if !m.Dialector.DontSupportRenameColumn {
			return m.Migrator.RenameColumn(value, oldName, newName)
		}

		var field *schema.Field
		if stmt.Schema != nil {
			if f := stmt.Schema.LookUpField(oldName); f != nil {
				oldName = f.DBName
				field = f
			}

			if f := stmt.Schema.LookUpField(newName); f != nil {
				newName = f.DBName
				field = f
			}
		}

		if field != nil {
			return m.DB.Exec(
				"ALTER TABLE ? CHANGE ? ? ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: oldName},
				clause.Column{Name: newName}, m.FullDataTypeOf(field),
			).Error
		}

		return fmt.Errorf("failed to look up field with name: %s", newName)
	})
}

func (m Migrator) RenameIndex(value interface{}, oldName, newName string) error {
	if !m.Dialector.DontSupportRenameIndex {
		return m.RunWithValue(value, func(stmt *gorm.Statement) error {
			return m.DB.Exec(
				"ALTER TABLE ? RENAME INDEX ? TO ?",
				clause.Table{Name: stmt.Table}, clause.Column{Name: oldName}, clause.Column{Name: newName},
			).Error
		})
	}

	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		err := m.DropIndex(value, oldName)
		if err != nil {
			return err
		}

		if stmt.Schema != nil {
			if idx := stmt.Schema.LookIndex(newName); idx == nil {
				if idx = stmt.Schema.LookIndex(oldName); idx != nil {
					opts := m.BuildIndexOptions(idx.Fields, stmt)
					values := []interface{}{clause.Column{Name: newName}, clause.Table{Name: stmt.Table}, opts}

					createIndexSQL := "CREATE "
					if idx.Class != "" {
						createIndexSQL += idx.Class + " "
					}
					createIndexSQL += "INDEX ? ON ??"

					if idx.Type != "" {
						createIndexSQL += " USING " + idx.Type
					}

					return m.DB.Exec(createIndexSQL, values...).Error
				}
			}
		}

		return m.CreateIndex(value, newName)
	})

}

func (m Migrator) DropTable(values ...interface{}) error {
	values = m.ReorderModels(values, false)
	return m.DB.Connection(func(tx *gorm.DB) error {
		tx.Exec("SET FOREIGN_KEY_CHECKS = 0;")
		for i := len(values) - 1; i >= 0; i-- {
			if err := m.RunWithValue(values[i], func(stmt *gorm.Statement) error {
				return tx.Exec("DROP TABLE IF EXISTS ? CASCADE", clause.Table{Name: stmt.Table}).Error
			}); err != nil {
				return err
			}
		}
		return tx.Exec("SET FOREIGN_KEY_CHECKS = 1;").Error
	})
}

func (m Migrator) DropConstraint(value interface{}, name string) error {
	return m.RunWithValue(value, func(stmt *gorm.Statement) error {
		constraint, chk, table := m.GuessConstraintAndTable(stmt, name)
		if chk != nil {
			return m.DB.Exec("ALTER TABLE ? DROP CHECK ?", clause.Table{Name: stmt.Table}, clause.Column{Name: chk.Name}).Error
		}
		if constraint != nil {
			name = constraint.Name
		}

		return m.DB.Exec(
			"ALTER TABLE ? DROP FOREIGN KEY ?", clause.Table{Name: table}, clause.Column{Name: name},
		).Error
	})
}

// ColumnTypes column types return columnTypes,error
func (m Migrator) ColumnTypes(value interface{}) ([]gorm.ColumnType, error) {
	columnTypes := make([]gorm.ColumnType, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			currentDatabase, table = m.CurrentSchema(stmt, stmt.Table)
			//columnTypeSQL          = "SELECT column_name, column_default, is_nullable = 'YES', data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale "
			columnTypeSQL = "SELECT column_name, column_default, is_nullable, data_type, character_maximum_length, column_type, column_key, extra, column_comment, numeric_precision, numeric_scale "

			rows, err = m.DB.Session(&gorm.Session{}).Table(table).Limit(1).Rows()
		)

		if err != nil {
			return err
		}

		rawColumnTypes, err := rows.ColumnTypes()

		if err != nil {
			return err
		}

		if err := rows.Close(); err != nil {
			return err
		}

		if !m.DisableDatetimePrecision {
			columnTypeSQL += ", datetime_precision "
		}
		columnTypeSQL += "FROM information_schema.columns WHERE is_nullable = 'true' AND table_schema = ? AND table_name = ? ORDER BY ORDINAL_POSITION"

		columns, rowErr := m.DB.Table(table).Raw(columnTypeSQL, currentDatabase, strings.ToUpper(table)).Rows()
		if rowErr != nil {
			return rowErr
		}

		defer columns.Close()

		for columns.Next() {
			var (
				column            migrator.ColumnType
				datetimePrecision sql.NullInt64
				extraValue        sql.NullString
				columnKey         sql.NullString
				values            = []interface{}{
					&column.NameValue, &column.DefaultValueValue, &column.NullableValue, &column.DataTypeValue, &column.LengthValue, &column.ColumnTypeValue, &columnKey, &extraValue, &column.CommentValue, &column.DecimalSizeValue, &column.ScaleValue,
				}
			)

			if !m.DisableDatetimePrecision { //禁用 datetime 精度
				values = append(values, &datetimePrecision)
			}

			if scanErr := columns.Scan(values...); scanErr != nil {
				return scanErr
			}

			column.PrimaryKeyValue = sql.NullBool{Bool: false, Valid: true}
			column.UniqueValue = sql.NullBool{Bool: false, Valid: true}
			switch columnKey.String {
			case "PRI":
				column.PrimaryKeyValue = sql.NullBool{Bool: true, Valid: true}
			case "UNI":
				column.UniqueValue = sql.NullBool{Bool: true, Valid: true}
			}

			if strings.Contains(extraValue.String, "auto_increment") {
				column.AutoIncrementValue = sql.NullBool{Bool: true, Valid: true}
			}

			column.DefaultValueValue.String = strings.Trim(column.DefaultValueValue.String, "'")
			if m.Dialector.DontSupportNullAsDefaultValue {
				// rewrite mariadb default value like other version
				if column.DefaultValueValue.Valid && column.DefaultValueValue.String == "NULL" {
					column.DefaultValueValue.Valid = false
					column.DefaultValueValue.String = ""
				}
			}

			if datetimePrecision.Valid {
				column.DecimalSizeValue = datetimePrecision
			}

			for _, c := range rawColumnTypes {
				if c.Name() == column.NameValue.String {
					column.SQLColumnType = c
					break
				}
			}

			columnTypes = append(columnTypes, column)
		}

		return nil
	})

	return columnTypes, err
}

func (m Migrator) CurrentDatabase() (name string) {
	//m.DB.Raw("cloudwave", 164).Scan(&name) //weip ?????? 翰云不支持 SELECT DATABASE()
	name = "test"
	baseName := strings.ToUpper(name)
	m.DB.Raw("SELECT SCHEMA_NAME from Information_schema.SCHEMATA where SCHEMA_NAME = ? limit 1", baseName).Scan(&name)
	return
}

func (m Migrator) GetTables() (tableList []string, err error) {
	err = m.DB.Raw("SELECT TABLE_NAME FROM information_schema.tables where TABLE_SCHEMA=?", m.CurrentDatabase()).
		Scan(&tableList).Error
	return
}

func (m Migrator) GetIndexes(value interface{}) ([]gorm.Index, error) {
	indexes := make([]gorm.Index, 0)
	err := m.RunWithValue(value, func(stmt *gorm.Statement) error {

		result := make([]*Index, 0)
		schema, table := m.CurrentSchema(stmt, stmt.Table)
		scanErr := m.DB.Table(table).Raw(indexSql, schema, table).Scan(&result).Error
		if scanErr != nil {
			return scanErr
		}
		indexMap, indexNames := groupByIndexName(result)

		for _, name := range indexNames {
			idx := indexMap[name]
			if len(idx) == 0 {
				continue
			}
			tempIdx := &migrator.Index{
				TableName: idx[0].TableName,
				NameValue: idx[0].IndexName,
				PrimaryKeyValue: sql.NullBool{
					Bool:  idx[0].IndexName == "PRIMARY",
					Valid: true,
				},
				UniqueValue: sql.NullBool{
					Bool:  idx[0].NonUnique == 0,
					Valid: true,
				},
			}
			for _, x := range idx {
				tempIdx.ColumnList = append(tempIdx.ColumnList, x.ColumnName)
			}
			indexes = append(indexes, tempIdx)
		}
		return nil
	})
	return indexes, err
}

// Index table index info
type Index struct {
	TableName  string `gorm:"column:TABLE_NAME"`
	ColumnName string `gorm:"column:COLUMN_NAME"`
	IndexName  string `gorm:"column:INDEX_NAME"`
	NonUnique  int32  `gorm:"column:NON_UNIQUE"`
}

func groupByIndexName(indexList []*Index) (map[string][]*Index, []string) {
	columnIndexMap := make(map[string][]*Index, len(indexList))
	indexNames := make([]string, 0, len(indexList))
	for _, idx := range indexList {
		if _, ok := columnIndexMap[idx.IndexName]; !ok {
			indexNames = append(indexNames, idx.IndexName)
		}
		columnIndexMap[idx.IndexName] = append(columnIndexMap[idx.IndexName], idx)
	}
	return columnIndexMap, indexNames
}

func (m Migrator) CurrentSchema(stmt *gorm.Statement, table string) (string, string) {
	if tables := strings.Split(table, `.`); len(tables) == 2 {
		return tables[0], tables[1]
	}
	m.DB = m.DB.Table(table)
	return m.CurrentDatabase(), table
}

func (m Migrator) GetTypeAliases(databaseTypeName string) []string {
	return typeAliasMap[databaseTypeName]
}

// TableType table type return tableType,error
func (m Migrator) TableType(value interface{}) (tableType gorm.TableType, err error) {
	var table migrator.TableType

	err = m.RunWithValue(value, func(stmt *gorm.Statement) error {
		var (
			values = []interface{}{
				&table.SchemaValue, &table.NameValue, &table.TypeValue, &table.CommentValue,
			}
			currentDatabase, tableName = m.CurrentSchema(stmt, stmt.Table)
			tableTypeSQL               = "SELECT table_schema, table_name, table_type, table_comment FROM information_schema.tables WHERE table_schema = ? AND table_name = ?"
		)

		row := m.DB.Table(tableName).Raw(tableTypeSQL, currentDatabase, tableName).Row()

		if scanErr := row.Scan(values...); scanErr != nil {
			return scanErr
		}

		return nil
	})

	return table, err
}

/////////////////////////////////////////////////////////////////////////////////////////////////////

// cloudwave
func (m Migrator) HasTable(value interface{}) bool {
	var count int64

	m.RunWithValue(value, func(stmt *gorm.Statement) error {
		currentDatabase := m.DB.Migrator().CurrentDatabase()
		return m.DB.Raw("SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?", strings.ToUpper(currentDatabase), strings.ToUpper(stmt.Table)).Row().Scan(&count)
	})

	return count > 0
}

func buildConstraint(constraint *schema.Constraint) (sql string, results []interface{}) {
	sql = "CONSTRAINT ? FOREIGN KEY ? REFERENCES ??"
	if constraint.OnDelete != "" {
		sql += " ON DELETE " + constraint.OnDelete
	}

	if constraint.OnUpdate != "" {
		sql += " ON UPDATE " + constraint.OnUpdate
	}

	var foreignKeys, references []interface{}
	for _, field := range constraint.ForeignKeys {
		foreignKeys = append(foreignKeys, clause.Column{Name: field.DBName})
	}

	for _, field := range constraint.References {
		references = append(references, clause.Column{Name: field.DBName})
	}
	results = append(results, clause.Table{Name: constraint.Name}, foreignKeys, clause.Table{Name: constraint.ReferenceSchema.Table}, references)
	return
}

// CreateTable create table in database for values
func (m Migrator) CreateTable(values ...interface{}) error {
	for _, value := range m.ReorderModels(values, false) {
		tx := m.DB.Session(&gorm.Session{})
		if err := m.RunWithValue(value, func(stmt *gorm.Statement) (err error) {
			var (
				createTableSQL          = "CREATE TABLE ? ("
				values                  = []interface{}{m.CurrentTable(stmt)}
				hasPrimaryKeyInDataType bool
			)

			for _, dbName := range stmt.Schema.DBNames {
				field := stmt.Schema.FieldsByDBName[dbName]
				if !field.IgnoreMigration {
					createTableSQL += "? ?"
					hasPrimaryKeyInDataType = hasPrimaryKeyInDataType || strings.Contains(strings.ToUpper(string(field.DataType)), "PRIMARY KEY")
					values = append(values, clause.Column{Name: dbName}, m.DB.Migrator().FullDataTypeOf(field))
					createTableSQL += ","
				}
			}

			if !hasPrimaryKeyInDataType && len(stmt.Schema.PrimaryFields) > 0 {
				createTableSQL += "PRIMARY KEY ?,"
				primaryKeys := make([]interface{}, 0, len(stmt.Schema.PrimaryFields))
				for _, field := range stmt.Schema.PrimaryFields {
					primaryKeys = append(primaryKeys, clause.Column{Name: field.DBName})
				}

				values = append(values, primaryKeys)
			}
			/*
				for _, idx := range stmt.Schema.ParseIndexes() {
					if m.CreateIndexAfterCreateTable {
						defer func(value interface{}, name string) {
							if err == nil {
								err = tx.Migrator().CreateIndex(value, name)
							}
						}(value, idx.Name)
					} else {
						if idx.Class != "" {
							createTableSQL += idx.Class + " "
						}
						createTableSQL += "INDEX ? ?"

						if idx.Comment != "" {
							createTableSQL += fmt.Sprintf(" COMMENT '%s'", idx.Comment)
						}

						if idx.Option != "" {
							createTableSQL += " " + idx.Option
						}

						createTableSQL += ","
						values = append(values, clause.Column{Name: idx.Name}, tx.Migrator().(migrator.BuildIndexOptionsInterface).BuildIndexOptions(idx.Fields, stmt))
					}
				}
			*/ //weip ?????? 建表时建IDX

			if !m.DB.DisableForeignKeyConstraintWhenMigrating && !m.DB.IgnoreRelationshipsWhenMigrating {
				for _, rel := range stmt.Schema.Relationships.Relations {
					if rel.Field.IgnoreMigration {
						continue
					}
					if constraint := rel.ParseConstraint(); constraint != nil {
						if constraint.Schema == stmt.Schema {
							sql, vars := buildConstraint(constraint)
							createTableSQL += sql + ","
							values = append(values, vars...)
						}
					}
				}
			}

			for _, chk := range stmt.Schema.ParseCheckConstraints() {
				createTableSQL += "CONSTRAINT ? CHECK (?),"
				values = append(values, clause.Column{Name: chk.Name}, clause.Expr{SQL: chk.Constraint})
			}

			createTableSQL = strings.TrimSuffix(createTableSQL, ",")

			createTableSQL += ")"

			if tableOption, ok := m.DB.Get("gorm:table_options"); ok {
				createTableSQL += fmt.Sprint(tableOption)
			}

			err = tx.Exec(createTableSQL, values...).Error
			return err
		}); err != nil {
			return err
		}
	}
	return nil
}

var regFullDataType = regexp.MustCompile(`\D*(\d+)\D?`)

// MigrateColumn migrate column
func (m Migrator) MigrateColumn(value interface{}, field *schema.Field, columnType gorm.ColumnType) error {
	// found, smart migrate
	fullDataType := strings.TrimSpace(strings.ToLower(m.DB.Migrator().FullDataTypeOf(field).SQL))
	realDataType := strings.ToLower(columnType.DatabaseTypeName())

	var (
		alterColumn bool
		isSameType  = fullDataType == realDataType
	)

	if !field.PrimaryKey {
		// check type
		if !strings.HasPrefix(fullDataType, realDataType) {
			// check type aliases
			aliases := m.DB.Migrator().GetTypeAliases(realDataType)
			for _, alias := range aliases {
				if strings.HasPrefix(fullDataType, alias) {
					isSameType = true
					break
				}
			}

			if !isSameType {
				alterColumn = true
			}
		}
	}

	if !isSameType {
		// check size
		if length, ok := columnType.Length(); (length * 8) != int64(field.Size) {
			if length > 0 && field.Size > 0 {
				alterColumn = true
			} else {
				// has size in data type and not equal
				// Since the following code is frequently called in the for loop, reg optimization is needed here
				matches2 := regFullDataType.FindAllStringSubmatch(fullDataType, -1)
				if !field.PrimaryKey &&
					(len(matches2) == 1 && matches2[0][1] != fmt.Sprint(length) && ok) {
					alterColumn = true
				}
			}
		}

		// check precision
		if precision, _, ok := columnType.DecimalSize(); ok && int64(field.Precision) != precision {
			//if regexp.MustCompile(fmt.Sprintf("[^0-9]%d[^0-9]", field.Precision)).MatchString(m.DataTypeOf(field)) {
			//alterColumn = true
			//} //weip ??????
		}
	}

	// check nullable
	if nullable, ok := columnType.Nullable(); ok && nullable == field.NotNull {
		// not primary key & database is nullable
		if !field.PrimaryKey && nullable {
			//alterColumn = true
		}
	}

	// check unique
	if unique, ok := columnType.Unique(); ok && unique != field.Unique {
		// not primary key
		if !field.PrimaryKey {
			//alterColumn = true
		}
	}

	// check default value
	if !field.PrimaryKey {
		currentDefaultNotNull := field.HasDefaultValue && (field.DefaultValueInterface != nil || !strings.EqualFold(field.DefaultValue, "NULL"))
		dv, dvNotNull := columnType.DefaultValue()
		if dvNotNull && !currentDefaultNotNull {
			// default value -> null
			alterColumn = true
		} else if !dvNotNull && currentDefaultNotNull {
			// null -> default value
			alterColumn = true
		} else if (field.GORMDataType != schema.Time && dv != field.DefaultValue) ||
			(field.GORMDataType == schema.Time && !strings.EqualFold(strings.TrimSuffix(dv, "()"), strings.TrimSuffix(field.DefaultValue, "()"))) {
			// default value not equal
			// not both null
			if currentDefaultNotNull || dvNotNull {
				alterColumn = true
			}
		}
	}

	// check comment
	if comment, ok := columnType.Comment(); ok && comment != field.Comment {
		// not primary key
		if !field.PrimaryKey {
			alterColumn = true
		}
	}

	if !m.DontSupportAlterColumn && alterColumn && !field.IgnoreMigration {
		return m.DB.Migrator().AlterColumn(value, field.DBName)
	}

	return nil
}
