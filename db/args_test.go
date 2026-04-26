package db

import "testing"

func testPlaintext(x, y int, payload string) *Plaintext {
	msg := new(Plaintext)
	msg.X = x
	msg.Y = y
	copy(msg.Message[:], []byte(payload))
	return msg
}

func TestInitializeUploadArgsSetsRouteRowAndEncryptedQueries(t *testing.T) {
	msg := testPlaintext(3, 7, "hello riposte")

	var args UploadArgs1
	shares, err := InitializeUploadArgs(&args, msg, false)
	if err != nil {
		t.Fatalf("InitializeUploadArgs failed: %v", err)
	}
	if args.RouteRow != msg.Y {
		t.Fatalf("expected RouteRow=%d, got %d", msg.Y, args.RouteRow)
	}
	if len(shares) != NUM_SERVERS {
		t.Fatalf("expected %d slot shares, got %d", NUM_SERVERS, len(shares))
	}

	for serv := 0; serv < len(args.Query); serv++ {
		var q InsertQuery1
		if err := DecryptQuery(serv, args.Query[serv], &q); err != nil {
			t.Fatalf("DecryptQuery(%d) failed: %v", serv, err)
		}
		if q.KeyIndex != serv {
			t.Fatalf("expected KeyIndex=%d, got %d", serv, q.KeyIndex)
		}
		if len(q.Keys) != TABLE_HEIGHT {
			t.Fatalf("expected %d keys, got %d", TABLE_HEIGHT, len(q.Keys))
		}
		if len(q.KeyMask) != TABLE_HEIGHT {
			t.Fatalf("expected %d key mask entries, got %d", TABLE_HEIGHT, len(q.KeyMask))
		}
	}
}

func TestSetUploadArgs2And3CarrySessionData(t *testing.T) {
	msg := testPlaintext(9, 11, "phase3")

	var up1 UploadArgs1
	msgShares, err := InitializeUploadArgs(&up1, msg, false)
	if err != nil {
		t.Fatalf("InitializeUploadArgs failed: %v", err)
	}

	up1Reply := &UploadReply1{Uuid: 42}
	copy(up1Reply.HashKey[:], []byte("0123456789abcdef0123456789abcdef"))

	msgInt, up2 := SetUploadArgs2(msgShares, &up1, up1Reply)
	if up2.Uuid != up1Reply.Uuid {
		t.Fatalf("expected UploadArgs2 UUID %d, got %d", up1Reply.Uuid, up2.Uuid)
	}
	if msgInt == nil || msgInt.Sign() == 0 {
		t.Fatalf("expected non-zero msgInt, got %v", msgInt)
	}
	for serv := 0; serv < NUM_SERVERS; serv++ {
		var q InsertQuery2
		if err := DecryptQuery(serv, up2.Query[serv], &q); err != nil {
			t.Fatalf("DecryptQuery upload2[%d] failed: %v", serv, err)
		}
		if q.MsgShare == nil {
			t.Fatalf("expected non-nil MsgShare for server %d", serv)
		}
	}

	up2Reply := &UploadReply2{}
	copy(up2Reply.Challenge[:], []byte("challenge-123456"))
	up3 := SetUploadArgs3(msg, msgInt, &up1, up1Reply, up2, up2Reply)
	if up3.Uuid != up1Reply.Uuid {
		t.Fatalf("expected UploadArgs3 UUID %d, got %d", up1Reply.Uuid, up3.Uuid)
	}
	for serv := 0; serv < NUM_SERVERS; serv++ {
		var q InsertQuery3
		if err := DecryptQuery(serv, up3.Query[serv], &q); err != nil {
			t.Fatalf("DecryptQuery upload3[%d] failed: %v", serv, err)
		}
		if q.TShare1 == nil || q.TShare2 == nil {
			t.Fatalf("expected non-nil T shares for server %d", serv)
		}
	}
}
