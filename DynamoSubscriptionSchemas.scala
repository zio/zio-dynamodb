//package com.disneystreaming.subscription.v2.infra.dynamo.formats
//
//import cats.Order
//import cats.data.NonEmptySet
//import cats.free.FreeApplicative
//import cats.implicits._
//import com.disneystreaming.subscription.v2.core.models.BundleType.BundleType
//import com.disneystreaming.subscription.v2.core.models.CSTState.CSTState
//import com.disneystreaming.subscription.v2.core.models.CSTSubscriptionReason.CSTSubscriptionReason
//import com.disneystreaming.subscription.v2.core.models.CancellationType.CancellationType
//import com.disneystreaming.subscription.v2.core.models.D2CState.D2CState
//import com.disneystreaming.subscription.v2.core.models.PaymentProvider.PaymentProvider
//import com.disneystreaming.subscription.v2.core.models.SourceProvider.SourceProvider
//import com.disneystreaming.subscription.v2.core.models.SourceType.SourceType
//import com.disneystreaming.subscription.v2.core.models.StackingStatus.StackingStatus
//import com.disneystreaming.subscription.v2.core.models.SubscriberState.SubscriberState
//import com.disneystreaming.subscription.v2.core.models.SubscriptionType.SubscriptionType
//import com.disneystreaming.subscription.v2.core.models.ThirdPartyState.ThirdPartyState
//import com.disneystreaming.subscription.v2.core.models._
//import com.disneystreaming.subscription.v2.core.models.entitlements.Entitlement
//import dynosaur.Schema
//import dynosaur.Schema.ReadError
//import shapeless.Generic
//import shapeless.HNil
//
//import java.time.Instant
//import scala.collection.immutable.SortedSet
//
//trait DynamoSubscriptionSchemas extends DynamoBaseSchema {
//
//  private[formats] implicit lazy val billingCycleSchema: Schema[BillingCycle] = Schema.record {
//    field =>
//      (
//        field("period", _.period),
//        field("timeUnit", _.timeUnit),
//      ).mapN(BillingCycle)
//  }
//
//  private[dynamo] implicit lazy val identityIdSchema: Schema[IdentityId] =
//    Schema[String].imap(IdentityId)(_.value)
//
//  private[dynamo] implicit lazy val subjectIdSchema: Schema[SubjectId] =
//    Schema[String].imap(SubjectId)(_.value)
//
//  private[dynamo] implicit lazy val billingProviderSchema: Schema[BillingProvider] =
//    Schema[String].imap(BillingProvider)(_.value)
//
//  private[formats] implicit lazy val cstTermSchema: Schema[CSTTerm] = Schema.record { field =>
//    (
//      field("subscriptionStartDate", _.subscriptionStartDate),
//      field("creationDate", _.creationDate),
//      field.opt("expirationDate", _.expirationDate),
//      field.opt("churnedDate", _.churnedDate),
//    ).mapN(CSTTerm.apply)
//  }
//
//  private[dynamo] implicit lazy val d2cRecurringTermSchema: Schema[D2CRecurringTerm] = Schema
//    .record { field =>
//      (
//        field("subscriptionStartDate", _.subscriptionStartDate),
//        field("purchaseDate", _.purchaseDate),
//        field("creationDate", _.creationDate),
//        field("nextBillingDate", _.nextBillingDate),
//        field("nextRenewalDate", _.nextRenewalDate),
//        field("transactionCount", _.transactionCount),
//        field("billingPhase", _.billingPhase),
//        field.opt("churnedDate", _.churnedDate),
//        field.opt("pausedDate", _.pausedDate),
//        field.opt("latestInvoiceId", _.latestInvoiceId),
//        field.opt("isFreeTrial", _.isFreeTrial),
//        field.opt("originalNextRenewalDate", _.originalNextRenewalDate),
//      ).mapN(D2CRecurringTerm)
//    }
//
//  private[formats] implicit lazy val d2cOneTimeTermSchema: Schema[D2COneTimeTerm] = Schema.record {
//    field =>
//      (
//        field("subscriptionStartDate", _.subscriptionStartDate),
//        field("purchaseDate", _.purchaseDate),
//        field("creationDate", _.creationDate),
//        field("subscriptionEndDate", _.subscriptionEndDate),
//      ).mapN(D2COneTimeTerm.apply)
//  }
//
//  private[formats] implicit lazy val trialDurationSchema: Schema[TrialDuration] = Schema.record {
//    field =>
//      (
//        field("period", _.period),
//        field("timeUnit", _.timeUnit),
//      ).mapN(TrialDuration.apply)
//  }
//
//  private[formats] implicit lazy val trialSchema: Schema[Trial] = Schema.record { field =>
//    field("duration", _.duration).map(Trial.apply)
//  }
//
//  private[dynamo] implicit lazy val cancellationSchema: Schema[Cancellation] = Schema.record {
//    field =>
//      field("type", _.`type`).map(Cancellation.apply)
//  }
//
//  private implicit lazy val entitlementSchema: Schema[Entitlement] = Schema.record { field =>
//    (
//      field("id", _.id),
//      field("name", _.name),
//      field.opt("desc", _.desc),
//      field.opt("partner", _.partner),
//    ).mapN(Entitlement.apply)
//  }
//
//  private[dynamo] implicit lazy val d2cRecurringProductSchema: Schema[D2CRecurringProduct] = Schema
//    .record { field =>
//      (
//        field("sku", _.sku),
//        field.opt("offerId", _.offerId),
//        field.opt("promotionId", _.promotionId),
//        field("id", _.id),
//        field("entitlements", _.entitlements),
//        field("redeemed", _.redeemed),
//        field.opt("trial", _.trial),
//        field("billingCycle", _.billingCycle),
//        field.opt("earlyAccess", _.earlyAccess.some).map(_.getOrElse(false)),
//        field.opt("categoryCodes", _.categoryCodes),
//        field.opt("name", _.name),
//        field.opt("bundleType", _.bundleType),
//      ).mapN(D2CRecurringProduct.apply)
//    }
//
//  private implicit lazy val stackingStatusSchema: Schema[StackingStatus] = enumSchema(
//    StackingStatus
//  )
//
//  implicit lazy val paymentProviderSchema: Schema[PaymentProvider] = enumSchema(
//    PaymentProvider
//  )
//
//  private def nonEmptyStringSetSchema[A: Order](
//    fromString: String => A
//  )(
//    stringify: A => String
//  ): Schema[NonEmptySet[A]] =
//    Schema[dynosaur.NonEmptySet[String]].imap(nesDyno =>
//      NonEmptySet.fromSetUnsafe(nesDyno.value.to(SortedSet)).map(fromString)
//    )(nes => dynosaur.NonEmptySet.fromCatsNonEmpty(nes.map(stringify)))
//
//  private implicit lazy val subscriptionIdNesSchema: Schema[NonEmptySet[SubscriptionId]] =
//    nonEmptyStringSetSchema(SubscriptionId.apply)(_.value)
//
//  private val stackedSchema: Schema[Stacked] = Schema.record { field =>
//    field.const("status", StackingStatus.STACKED) *>
//      (
//        field("overlappingSubscriptions", _.overlappingSubscriptions),
//        field.opt("previouslyStackedByProvider", _.previouslyStackedByProvider),
//        field("stateBeforeStacking", _.stateBeforeStacking),
//      ).mapN(Stacked.apply)
//  }
//
//  private val doubleBilledSchema: Schema[DoubleBilledStacking] = Schema.record { field =>
//    field.const("status", StackingStatus.DOUBLE_BILLED) *>
//      (
//        field("overlappingSubscriptions", _.overlappingSubscriptions),
//        field.opt("previouslyStackedByProvider", _.previouslyStackedByProvider),
//      ).mapN(DoubleBilledStacking.apply)
//  }
//
//  private val discountedSchema: Schema[DiscountedStacking] = Schema.record { field =>
//    field.const("status", StackingStatus.DISCOUNTED) *>
//      (
//        field("overlappingSubscriptions", _.overlappingSubscriptions),
//        field.opt("previouslyStackedByProvider", _.previouslyStackedByProvider),
//      ).mapN(DiscountedStacking.apply)
//  }
//
//  private val previouslyStackedSchema: Schema[PreviouslyStacked] = Schema.record { field =>
//    field("previouslyStackedByProvider", _.previouslyStackedByProvider)
//      .map(PreviouslyStacked)
//  }
//
//  implicit lazy val stackingSchema: Schema[Stacking] = Schema.oneOf { alt =>
//    alt(stackedSchema) |+|
//      alt(doubleBilledSchema) |+|
//      alt(discountedSchema) |+|
//      alt(previouslyStackedSchema)
//  }
//
//  implicit lazy val d2cOneTimeProductSchema: Schema[D2COneTimeProduct] = Schema.record { field =>
//    (
//      field("sku", _.sku),
//      field.opt("offerId", _.offerId),
//      field.opt("promotionId", _.promotionId),
//      field("id", _.id),
//      field("entitlements", _.entitlements),
//      field("redeemed", _.redeemed),
//      field.opt("trial", _.trial),
//      field.opt("earlyAccess", _.earlyAccess.some).map(_.getOrElse(false)),
//      field.opt("categoryCodes", _.categoryCodes),
//      field.opt("name", _.name),
//      field.opt("bundleType", _.bundleType),
//    ).mapN(D2COneTimeProduct.apply)
//  }
//
//  implicit lazy val thirdPartyProductSchema: Schema[ThirdPartyProduct] = Schema.record { field =>
//    (
//      field("id", _.id),
//      field("offerId", _.offerId),
//      field.opt("promotionId", _.promotionId),
//    ).mapN(ThirdPartyProduct.apply)
//  }
//
//  implicit lazy val thirdPartyTermSchema: Schema[ThirdPartyTerm] = Schema.record { field =>
//    (
//      field("subscriptionStartDate", _.subscriptionStartDate),
//      field("creationDate", _.creationDate),
//      field("purchaseDate", _.purchaseDate),
//      field.opt("chargeThroughDateDate", _.chargeThroughDate),
//    ).mapN(ThirdPartyTerm.apply)
//  }
//
//  def const[A <: AnyRef: Schema](value: A): Schema[value.type] =
//    Schema[A].imapErr[value.type] { a =>
//      Either.cond(
//        a == value,
//        right = value,
//        left = Schema.ReadError(s"Value wasn't exactly $value (got $a instead)"),
//      )
//    }(_ => value)
//
//  implicit lazy val thirdPartyCancellationReason: Schema[ThirdPartyCancellationReason] =
//    Schema[String].imapErr { s =>
//      ThirdPartyCancellationReason
//        .fromString(s)
//        .toRight(ReadError(s"'$s' not a valid value for ThirdPartyCancellationReason"))
//    }(_.toString)
//
//  implicit lazy val subscriptionIdSchema: Schema[SubscriptionId] =
//    Schema[String].imap(SubscriptionId.apply)(_.value)
//
//  private[formats] def d2cRecurringSubscriptionSchema(
//    getTTL: Instant => Instant
//  ): Schema[D2CRecurringSubscription] =
//    Schema
//      .record { field =>
//        // Workaround for 23+ fields in a single record
//        import ShapelessSchemas._
//
//        field.const("subscriptionType", SubscriptionType.D2CRecurring) *>
//          makeTTLField[D2CRecurringSubscription](getTTL) *>
//          (
//            field("id", _.id) ::
//              field.opt("accountId", _.accountId) ::
//              field("subscriptionGroupId", _.subscriptionGroupId) ::
//              field("product", _.product) ::
//              field("term", _.term) ::
//              field("source", _.source) ::
//              field("state", _.state) ::
//              field.opt("cancellation", _.cancellation) ::
//              field("lastUpdatedDate", _.lastUpdatedDate) ::
//              field.opt("walletId", _.walletId) ::
//              field.opt("paymentMethodId", _.paymentMethodId) ::
//              field.opt("stacking", _.stacking) ::
//              field("partner", _.partner) ::
//              field("isTest", _.isTest) ::
//              field.opt("isPendingPaymentConfirmation", _.isPendingPaymentConfirmation) ::
//              field.opt("orderId", _.orderId) ::
//              field.opt("identityId", _.identityId) ::
//              field.opt("isPendingInitialPayment", _.isPendingInitialPayment) ::
//              field.opt("paymentProvider", _.paymentProvider) ::
//              field.opt("purchasePlatform", _.purchasePlatform) ::
//              field.opt("previousSubscriptionId", _.previousSubscriptionId) ::
//              field.opt("nextSubscriptionId", _.nextSubscriptionId) ::
//              field.pure(SubscriptionType.D2CRecurring) ::
//              field.pure(HNil)
//          ).map(Generic[D2CRecurringSubscription].from)
//      }
//
//  private[formats] def d2cOneTimeSubscriptionSchema(
//    getTTL: Instant => Instant
//  ): Schema[D2COneTimeSubscription] =
//    Schema.record { field =>
//      field.const("subscriptionType", SubscriptionType.D2COneTime) *>
//        makeTTLField[D2COneTimeSubscription](getTTL) *>
//        (
//          field("id", _.id),
//          field.opt("accountId", _.accountId),
//          field("subscriptionGroupId", _.subscriptionGroupId),
//          field("product", _.product),
//          field("term", _.term),
//          field("source", _.source),
//          field("state", _.state),
//          field("lastUpdatedDate", _.lastUpdatedDate),
//          field.opt("walletId", _.walletId),
//          field.opt("paymentMethodId", _.paymentMethodId),
//          field("partner", _.partner),
//          field("isTest", _.isTest),
//          field.opt("identityId", _.identityId),
//          field.opt("isPendingInitialPayment", _.isPendingInitialPayment),
//          field.opt("paymentProvider", _.paymentProvider),
//          field.opt("purchasePlatform", _.purchasePlatform),
//          field.pure(SubscriptionType.D2COneTime),
//        ).mapN(D2COneTimeSubscription.apply)
//
//    }
//
//  private[formats] def thirdPartySubscriptionSchema(
//    getTTL: Instant => Instant
//  ): Schema[ThirdPartySubscription] =
//    Schema.record { field =>
//      field.const("subscriptionType", SubscriptionType.ThirdParty) *>
//        makeTTLField[ThirdPartySubscription](getTTL) *>
//        (
//          field("id", _.id),
//          field.opt("accountId", _.accountId),
//          field.opt("identityId", _.identityId),
//          field("subjectId", _.subjectId),
//          field("billingProvider", _.billingProvider),
//          field("product", _.product),
//          field.opt("cancellationReason", _.cancellationReason),
//          field("state", _.state),
//          field("term", _.term),
//          field("source", _.source),
//          field("partner", _.partner),
//          field("isTest", _.isTest),
//          field("lastUpdatedDate", _.lastUpdatedDate),
//        ).mapN(ThirdPartySubscription.apply)
//
//    }
//
//  private[formats] implicit lazy val cstProductSchema: Schema[CSTProduct] = Schema.record { field =>
//    (
//      field("sku", _.sku),
//      field("id", _.id),
//      field("entitlements", _.entitlements),
//      field.opt("earlyAccess", _.earlyAccess.some).map(_.getOrElse(false)),
//      field.opt("categoryCodes", _.categoryCodes),
//      field.opt("name", _.name),
//      field.opt("offerId", _.offerId),
//    ).mapN(CSTProduct.apply)
//  }
//
//  private implicit lazy val caseIdSchema: Schema[CaseId] =
//    Schema[String].imap(CaseId.apply)(_.value)
//
//  private def makeTTLField[S <: Subscription](
//    getTTL: Instant => Instant
//  ): FreeApplicative[Schema.structure.Field[S, *], Unit] =
//    Schema
//      .field[S]
//      .opt(
//        "ttl",
//        sub =>
//          if (sub.isTest)
//            Some(getTTL(sub.creationDate).getEpochSecond)
//          else
//            None,
//      )
//      .void
//
//  private[formats] def cstSubscriptionSchema(getTTL: Instant => Instant): Schema[CSTSubscription] =
//    Schema.record { field =>
//      field.const("subscriptionType", SubscriptionType.CST) *>
//        makeTTLField[CSTSubscription](getTTL) *>
//        (
//          field("id", _.id),
//          field.opt("accountId", _.accountId),
//          field("subscriptionGroupId", _.subscriptionGroupId),
//          field("product", _.product),
//          field("term", _.term),
//          field("source", _.source),
//          field("state", _.state),
//          field.opt("cancellation", _.cancellation),
//          field("lastUpdatedDate", _.lastUpdatedDate),
//          field("partner", _.partner),
//          field("isTest", _.isTest),
//          field.opt("reason", _.reason),
//          field.opt("caseId", _.caseId),
//          field.opt("identityId", _.identityId),
//          field.pure(SubscriptionType.CST),
//        ).mapN(CSTSubscription.apply)
//
//    }
//
//  private[formats] implicit lazy val subscriptionTypeSchema: Schema[SubscriptionType] = Schema
//    .oneOf[SubscriptionType] { alt =>
//      val constantD2C =
//        Schema
//          .string
//          .imapErr(t =>
//            Either.cond(
//              t == "D2C",
//              right = SubscriptionType.D2CRecurring,
//              left = Schema.ReadError("value wasn't D2C"),
//            )
//          )(_.toString)
//
//      alt(constantD2C) |+| alt(enumSchema(SubscriptionType))
//    }
//
//  def subscriptionSchemaWithTTL(getTTL: Instant => Instant): Schema[Subscription] =
//    Schema.oneOf { alt =>
//      alt(d2cOneTimeSubscriptionSchema(getTTL)) |+|
//        alt(d2cRecurringSubscriptionSchema(getTTL)) |+|
//        alt(cstSubscriptionSchema(getTTL)) |+|
//        alt(thirdPartySubscriptionSchema(getTTL))
//    }
//
//  private[dynamo] implicit lazy val accountIdSchema: Schema[AccountId] =
//    Schema[String].imap(AccountId.apply)(_.value)
//
//  private[formats] implicit lazy val redeemedSchema: Schema[Redeemed] = Schema.record { field =>
//    (
//      field("campaignCode", _.campaignCode),
//      field("voucherCode", _.voucherCode),
//      field.opt("redemptionCode", _.redemptionCode),
//    ).mapN(Redeemed.apply)
//  }
//
//  private implicit lazy val sourceTypeSchema: Schema[SourceType] = enumSchema(SourceType)
//
//  private implicit lazy val sourceProviderSchema: Schema[SourceProvider] = enumSchema(
//    SourceProvider
//  )
//
//  private implicit lazy val sourceSchema: Schema[Source] = Schema.record { field =>
//    (
//      field("type", _.`type`),
//      field("provider", _.`provider`),
//      field.opt("subType", _.subType),
//      field("ref", _.ref),
//      field.opt("salesPlatform", _.salesPlatform),
//    ).mapN(Source.apply)
//  }
//
//  private[formats] implicit lazy val sourceRefSchema: Schema[SourceRef] =
//    Schema[String].imap(SourceRef.apply)(_.value)
//
//  private[formats] implicit lazy val skuSchema: Schema[SKU] = Schema[String].imap(SKU)(_.value)
//
//  private[dynamo] implicit lazy val d2cStateSchema: Schema[D2CState] = enumSchema(D2CState)
//
//  private[dynamo] implicit lazy val cstStateSchema: Schema[CSTState] = enumSchema(CSTState)
//
//  private[dynamo] implicit lazy val thirdPartStateSchema: Schema[ThirdPartyState] = enumSchema(
//    ThirdPartyState
//  )
//
//  private implicit lazy val subscriberStateSchema: Schema[SubscriberState] = enumSchema(
//    SubscriberState
//  )
//
//  private[dynamo] implicit lazy val stateSchema: Schema[State#Value] = Schema.oneOf { alt =>
//    alt(Schema[D2CState]) |+|
//      alt(Schema[CSTState]) |+|
//      alt(Schema[SubscriberState])
//  }
//
//  private implicit lazy val reasonTypeSchema: Schema[CSTSubscriptionReason] = enumSchema(
//    CSTSubscriptionReason
//  )
//
//  private implicit lazy val cancellationTypeSchema: Schema[CancellationType] = enumSchema(
//    CancellationType
//  )
//
//  private implicit lazy val bundleTypeSchema: Schema[BundleType] = enumSchema(BundleType)
//}
