/*
 * Copyright 2021-2026 John A. De Goes and the ZIO Contributors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package zio.dynamodb

import java.util.{ HashMap => JHashMap, LinkedHashMap => JLinkedHashMap, Map => JMap }

/**
 * An immutable Scala Map view over an underlying java.util.Map.
 * Satisfies scala.collection.immutable.Map without copying the underlying data.
 * updated/removed are copy-on-write via cloneFn so callers observe standard immutable semantics.
 * cloneFn must produce the same concrete type as underlying (HashMap or LinkedHashMap).
 *
 * Shared by AttrMap (String keys) and AttributeValue.Map (AttributeValue.String keys).
 */
private[dynamodb] final class JMapView[K, V <: AnyRef](
  private[dynamodb] val underlying: JMap[K, V],
  private val cloneFn: JMap[K, V] => JMap[K, V]
) extends scala.collection.immutable.AbstractMap[K, V] {

  override def get(key: K): Option[V] = {
    val v = underlying.get(key)
    if (v eq null) None else Some(v)
  }

  private[dynamodb] def getNullable(key: K): V = underlying.get(key)

  override def iterator: Iterator[(K, V)] = {
    val it = underlying.entrySet().iterator()
    new Iterator[(K, V)] {
      def hasNext: Boolean = it.hasNext
      def next(): (K, V)   = { val e = it.next(); (e.getKey, e.getValue) }
    }
  }

  override def updated[V1 >: V](key: K, value: V1): Map[K, V1] = {
    val copy = cloneFn(underlying)
    copy.put(key, value.asInstanceOf[V])
    new JMapView(copy, cloneFn).asInstanceOf[Map[K, V1]]
  }

  override def removed(key: K): Map[K, V] = {
    val copy = cloneFn(underlying)
    copy.remove(key)
    new JMapView(copy, cloneFn)
  }

  override def size: Int                 = underlying.size
  override def isEmpty: Boolean          = underlying.isEmpty
  override def contains(key: K): Boolean = underlying.containsKey(key)
}

private[dynamodb] object JMapView {

  object hash {
    def builder[K, V <: AnyRef]: Builder[K, V] =
      new Builder(new JHashMap[K, V](), (m: JMap[K, V]) => new JHashMap[K, V](m))

    def single[K, V <: AnyRef](key: K, value: V): Map[K, V] = {
      val m = new JHashMap[K, V](2)
      m.put(key, value)
      new JMapView(m, (mm: JMap[K, V]) => new JHashMap[K, V](mm))
    }
  }

  object linked {
    def builder[K, V <: AnyRef]: Builder[K, V] =
      new Builder(new JLinkedHashMap[K, V](), (m: JMap[K, V]) => new JLinkedHashMap[K, V](m))

    def single[K, V <: AnyRef](key: K, value: V): Map[K, V] = {
      val m = new JLinkedHashMap[K, V](2)
      m.put(key, value)
      new JMapView(m, (mm: JMap[K, V]) => new JLinkedHashMap[K, V](mm))
    }
  }

  final class Builder[K, V <: AnyRef](
    private val underlying: JMap[K, V],
    private val cloneFn: JMap[K, V] => JMap[K, V]
  ) {
    def addOne(key: K, value: V): Builder[K, V] = {
      underlying.put(key, value)
      this
    }

    def ++=(entries: Iterable[(K, V)]): Builder[K, V] = {
      entries.foreach { case (k, v) => underlying.put(k, v) }
      this
    }

    def result: Map[K, V] = new JMapView[K, V](underlying, cloneFn)

    def clear(): Unit = underlying.clear()
  }
}
