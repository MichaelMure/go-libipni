package httpsync

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/hashicorp/go-multierror"
	"github.com/ipfs/go-cid"
	"github.com/ipld/go-ipld-prime"
	ic "github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/ipni/go-libipni/announce"
	"github.com/ipni/go-libipni/announce/message"
	"github.com/ipni/go-libipni/dagsync"
)

// Note: imho, one reason for unnecessary complexity is that a publisher has to do both DAG publishing AND announce.
// It occurs to me that the announcing part is already quite well separated in the `announce` package, and inserting
// it here only bridge things for not much reason. The caller side could instead setup DAG publishing and announce
// separately, and announce a new head when this publisher succeeds.

type Remote interface {
	WriteFile(ctx context.Context, path string, data []byte) error
	ReadFile(ctx context.Context, path string) (io.ReadCloser, error)
	Addrs() []multiaddr.Multiaddr
	Close() error
}

var _ dagsync.Publisher = &RemotePublisher{}

type RemotePublisher struct {
	remote  Remote
	peerId  peer.ID
	privKey ic.PrivKey
	lsys    ipld.LinkSystem

	senders   []announce.Sender
	extraData []byte
}

func NewRemotePublisher(remote Remote, privKey ic.PrivKey, options ...Option) (*RemotePublisher, error) {
	opts, err := getOpts(options)
	if err != nil {
		return nil, err
	}

	if privKey == nil {
		return nil, errors.New("private key required to sign head requests")
	}
	peerID, err := peer.IDFromPrivateKey(privKey)
	if err != nil {
		return nil, fmt.Errorf("could not get peer id from private key: %w", err)
	}

	return &RemotePublisher{
		remote:    remote,
		peerId:    peerID,
		privKey:   privKey,
		lsys:      mkLinkSystem(remote),
		senders:   opts.senders,
		extraData: opts.extraData,
	}, nil
}

// Addrs returns the addresses, as []multiaddress, that the Publisher is
// listening on.
func (p *RemotePublisher) Addrs() []multiaddr.Multiaddr {
	return p.remote.Addrs()
}

// ID returns the p2p peer ID of the Publisher.
func (p *RemotePublisher) ID() peer.ID {
	return p.peerId
}

// Protocol returns the multihash protocol ID of the transport used by the
// publisher.
func (p *RemotePublisher) Protocol() int {
	return multiaddr.P_HTTP
}

func (p *RemotePublisher) SetRoot(ctx context.Context, root cid.Cid) error {
	marshalledMsg, err := newEncodedSignedHead(root, p.privKey)
	if err != nil {
		return err
	}
	return p.remote.WriteFile(ctx, "/head", marshalledMsg)
}

// UpdateRoot does a SetRoot and announces.
func (p *RemotePublisher) UpdateRoot(ctx context.Context, root cid.Cid) error {
	err := p.SetRoot(ctx, root)
	if err != nil {
		return err
	}

	log.Debugf("Publishing CID and addresses over HTTP: %s", root)
	msg := message.Message{
		Cid:       root,
		ExtraData: p.extraData,
	}
	msg.SetAddrs(p.Addrs())

	var errs error
	for _, sender := range p.senders {
		if err := sender.Send(ctx, msg); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}

func (p *RemotePublisher) Close() error {
	var errs error
	for _, sender := range p.senders {
		if err := sender.Close(); err != nil {
			errs = multierror.Append(errs, err)
		}
	}
	return errs
}
