package binlog

import (
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
	// 更新会有两条记录，暂时只取一条
	for k, v := range rowsEvent.Rows {
		if k%2 == 1 && rowsEvent.Action == canal.UpdateAction {
			continue
		}
		e.srv.syncCh <- bulkRequest{Record: fmt.Sprintf("%v", reflect.ValueOf(v[0])), Table: rowsEvent.Table.Name, Action: rowsEvent.Action}
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
