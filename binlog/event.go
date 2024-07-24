package binlog

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"reflect"
)

var _ canal.EventHandler = (*event)(nil)

type gtidSetSaver struct {
	GtidSet string
}

type bulkRequest struct {
	Record string
	Table  string
	Action string
	Values string
}

type event struct {
	srv *Server
}

func (e *event) OnRotate(eventHeader *replication.EventHeader, rotateEvent *replication.RotateEvent) error {
	return nil
}

func (e *event) OnTableChanged(eventHeader *replication.EventHeader, schema string, table string) error {
	return nil
}

func (e *event) OnDDL(eventHeader *replication.EventHeader, nextPos mysql.Position, queryEvent *replication.QueryEvent) error {
	return nil
}

func (e *event) OnXID(eventHeader *replication.EventHeader, nextPos mysql.Position) error {
	return nil
}

func (e *event) OnRow(rowsEvent *canal.RowsEvent) error {

	for k, v := range rowsEvent.Rows {
		// 更新会有两条记录，暂时只取更新后的那条数据
		if rowsEvent.Action == canal.UpdateAction && k%2 == 0 {
			continue
		}

		// 识别列对应值
		values := make(map[string]interface{}, 0)
		for _, column := range rowsEvent.Table.Columns {
			value, _ := rowsEvent.Table.GetColumnValue(column.Name, v)
			values[column.Name] = value
		}

		// 转发给方法处理
		valuesJson, _ := json.Marshal(values)
		e.srv.syncCh <- bulkRequest{
			Record: fmt.Sprintf("%v", reflect.ValueOf(v[0])),
			Table:  rowsEvent.Table.Name,
			Action: rowsEvent.Action,
			Values: string(valuesJson),
		}
	}

	return e.srv.ctx.Err()
}

func (e *event) OnGTID(eventHeader *replication.EventHeader, gtidEvent mysql.BinlogGTIDEvent) error {
	return nil
}

func (e *event) OnPosSynced(eventHeader *replication.EventHeader, pos mysql.Position, gtidSet mysql.GTIDSet, force bool) error {
	e.srv.syncCh <- gtidSetSaver{gtidSet.String()}
	return e.srv.ctx.Err()

}

func (e *event) OnRowsQueryEvent(rqe *replication.RowsQueryEvent) error {
	return nil
}

func (e *event) String() string {
	return "EventHandler"
}
